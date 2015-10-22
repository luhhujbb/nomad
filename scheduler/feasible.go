package scheduler

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/hashicorp/go-version"
	"github.com/hashicorp/nomad/nomad/structs"
)

// FeasibleIterator is used to iteratively yield nodes that
// match feasibility constraints. The iterators may manage
// some state for performance optimizations.
type FeasibleIterator interface {
	// Next yields a feasible node or nil if exhausted
	Next() *structs.Node

	// Reset is invoked when an allocation has been placed
	// to reset any stale state.
	Reset()
}

// StaticIterator is a FeasibleIterator which returns nodes
// in a static order. This is used at the base of the iterator
// chain only for testing due to deterministic behavior.
type StaticIterator struct {
	ctx    Context
	nodes  []*structs.Node
	offset int
	seen   int
}

// NewStaticIterator constructs a random iterator from a list of nodes
func NewStaticIterator(ctx Context, nodes []*structs.Node) *StaticIterator {
	iter := &StaticIterator{
		ctx:   ctx,
		nodes: nodes,
	}
	return iter
}

func (iter *StaticIterator) Next() *structs.Node {
	// Check if exhausted
	n := len(iter.nodes)
	if iter.offset == n || iter.seen == n {
		if iter.seen != n {
			iter.offset = 0
		} else {
			return nil
		}
	}

	// Return the next offset
	offset := iter.offset
	iter.offset += 1
	iter.seen += 1
	iter.ctx.Metrics().EvaluateNode()
	return iter.nodes[offset]
}

func (iter *StaticIterator) Reset() {
	iter.seen = 0
}

func (iter *StaticIterator) SetNodes(nodes []*structs.Node) {
	iter.nodes = nodes
	iter.offset = 0
	iter.seen = 0
}

// NewRandomIterator constructs a static iterator from a list of nodes
// after applying the Fisher-Yates algorithm for a random shuffle. This
// is applied in-place
func NewRandomIterator(ctx Context, nodes []*structs.Node) *StaticIterator {
	// shuffle with the Fisher-Yates algorithm
	shuffleNodes(nodes)

	// Create a static iterator
	return NewStaticIterator(ctx, nodes)
}

// DriverIterator is a FeasibleIterator which returns nodes that
// have the drivers necessary to scheduler a task group.
type DriverIterator struct {
	ctx     Context
	source  FeasibleIterator
	drivers map[string]struct{}
}

// NewDriverIterator creates a DriverIterator from a source and set of drivers
func NewDriverIterator(ctx Context, source FeasibleIterator, drivers map[string]struct{}) *DriverIterator {
	iter := &DriverIterator{
		ctx:     ctx,
		source:  source,
		drivers: drivers,
	}
	return iter
}

func (iter *DriverIterator) SetDrivers(d map[string]struct{}) {
	iter.drivers = d
}

func (iter *DriverIterator) Next() *structs.Node {
	for {
		// Get the next option from the source
		option := iter.source.Next()
		if option == nil {
			return nil
		}

		// Use this node if possible
		if iter.hasDrivers(option) {
			return option
		}
		iter.ctx.Metrics().FilterNode(option, "missing drivers")
	}
}

func (iter *DriverIterator) Reset() {
	iter.source.Reset()
}

// hasDrivers is used to check if the node has all the appropriate
// drivers for this task group. Drivers are registered as node attribute
// like "driver.docker=1" with their corresponding version.
func (iter *DriverIterator) hasDrivers(option *structs.Node) bool {
	for driver := range iter.drivers {
		driverStr := fmt.Sprintf("driver.%s", driver)
		value, ok := option.Attributes[driverStr]
		if !ok {
			return false
		}

		enabled, err := strconv.ParseBool(value)
		if err != nil {
			iter.ctx.Logger().
				Printf("[WARN] scheduler.DriverIterator: node %v has invalid driver setting %v: %v",
				option.ID, driverStr, value)
			return false
		}

		if !enabled {
			return false
		}
	}
	return true
}

// DynamicConstraintIterator is a FeasibleIterator which returns nodes that
// match constraints that are not static such as Node attributes but are
// effected by alloc placements. Examples are unique and tenancy constraints.
// This is used to filter on job and task group constraints.
type DynamicConstraintIterator struct {
	ctx    Context
	source FeasibleIterator
	tg     *structs.TaskGroup
	job    *structs.Job

	// Store whether the Job or TaskGroup has unique constraints so they don't
	// have to be calculated every time Next() is called.
	tgUnique  bool
	jobUnique bool
}

// NewDynamicConstraintIterator creates a DynamicConstraintIterator from a
// source.
func NewDynamicConstraintIterator(ctx Context, source FeasibleIterator) *DynamicConstraintIterator {
	iter := &DynamicConstraintIterator{
		ctx:    ctx,
		source: source,
	}
	return iter
}

func (iter *DynamicConstraintIterator) SetTaskGroup(tg *structs.TaskGroup) {
	iter.tg = tg
	iter.tgUnique = iter.hasUniqueConstraint(tg.Constraints)
}

func (iter *DynamicConstraintIterator) SetJob(job *structs.Job) {
	iter.job = job
	iter.jobUnique = iter.hasUniqueConstraint(job.Constraints)
}

func (iter *DynamicConstraintIterator) hasUniqueConstraint(constraints []*structs.Constraint) bool {
	if constraints == nil {
		return false
	}

	for _, con := range constraints {
		if con.Operand == "unique" {
			return true
		}
	}
	return false
}

func (iter *DynamicConstraintIterator) Next() *structs.Node {
	if iter.job == nil {
		iter.ctx.Logger().Printf("[ERR] sched.dynamic-constraint: job not set")
		return nil
	}

	if iter.tg == nil {
		iter.ctx.Logger().Printf("[ERR] sched.dynamic-constraint: task group not set")
		return nil
	}

	for {
		// Get the next option from the source
		option := iter.source.Next()

		// Hot-path if the option is nil or there are no unique constraints.
		if option == nil || (!iter.jobUnique && !iter.tgUnique) {
			return option
		}

		if !iter.satisfiesUnique(option, iter.jobUnique) {
			iter.ctx.Metrics().FilterNode(option, "unique")
			continue
		}

		return option
	}
}

// satisfiesUnique checks if the node satisfies a unique constraint either
// specified at the job level or the TaskGroup level.
func (iter *DynamicConstraintIterator) satisfiesUnique(option *structs.Node, job bool) bool {
	// Get the proposed allocations
	proposed, err := iter.ctx.ProposedAllocs(option.ID)
	if err != nil {
		iter.ctx.Logger().Printf(
			"[ERR] sched.dynamic-constraint: failed to get proposed allocations: %v", err)
		return false
	}

	// Skip the node if the task group has already been allocated on it.
	for _, alloc := range proposed {
		jobCollision := alloc.JobID == iter.job.ID
		taskCollision := alloc.TaskGroup == iter.tg.Name

		// If the job has a unique constraint we only need an alloc collision on
		// the JobID but if the constraint is on the TaskGroup then we need both
		// a job and TaskGroup collision.
		jobInvalid := job && jobCollision
		tgInvalid := !job && jobCollision && taskCollision
		if jobInvalid || tgInvalid {
			return false
		}
	}

	return true
}

func (iter *DynamicConstraintIterator) Reset() {
	iter.source.Reset()
}

// ConstraintIterator is a FeasibleIterator which returns nodes
// that match a given set of constraints. This is used to filter
// on job, task group, and task constraints.
type ConstraintIterator struct {
	ctx         Context
	source      FeasibleIterator
	constraints []*structs.Constraint
}

// NewConstraintIterator creates a ConstraintIterator from a source and set of constraints
func NewConstraintIterator(ctx Context, source FeasibleIterator, constraints []*structs.Constraint) *ConstraintIterator {
	iter := &ConstraintIterator{
		ctx:         ctx,
		source:      source,
		constraints: constraints,
	}
	return iter
}

func (iter *ConstraintIterator) SetConstraints(c []*structs.Constraint) {
	iter.constraints = c
}

func (iter *ConstraintIterator) Next() *structs.Node {
	for {
		// Get the next option from the source
		option := iter.source.Next()
		if option == nil {
			return nil
		}

		// Use this node if possible
		if iter.meetsConstraints(option) {
			return option
		}
	}
}

func (iter *ConstraintIterator) Reset() {
	iter.source.Reset()
}

func (iter *ConstraintIterator) meetsConstraints(option *structs.Node) bool {
	for _, constraint := range iter.constraints {
		if !iter.meetsConstraint(constraint, option) {
			iter.ctx.Metrics().FilterNode(option, constraint.String())
			return false
		}
	}
	return true
}

func (iter *ConstraintIterator) meetsConstraint(constraint *structs.Constraint, option *structs.Node) bool {
	// Only enforce hard constraints, soft constraints are used for ranking
	if !constraint.Hard {
		return true
	}

	// Resolve the targets
	lVal, ok := resolveConstraintTarget(constraint.LTarget, option)
	if !ok {
		return false
	}
	rVal, ok := resolveConstraintTarget(constraint.RTarget, option)
	if !ok {
		return false
	}

	// Check if satisfied
	return checkConstraint(iter.ctx, constraint.Operand, lVal, rVal)
}

// resolveConstraintTarget is used to resolve the LTarget and RTarget of a Constraint
func resolveConstraintTarget(target string, node *structs.Node) (interface{}, bool) {
	// If no prefix, this must be a literal value
	if !strings.HasPrefix(target, "$") {
		return target, true
	}

	// Handle the interpolations
	switch {
	case "$node.id" == target:
		return node.ID, true

	case "$node.datacenter" == target:
		return node.Datacenter, true

	case "$node.name" == target:
		return node.Name, true

	case strings.HasPrefix(target, "$attr."):
		attr := strings.TrimPrefix(target, "$attr.")
		val, ok := node.Attributes[attr]
		return val, ok

	case strings.HasPrefix(target, "$meta."):
		meta := strings.TrimPrefix(target, "$meta.")
		val, ok := node.Meta[meta]
		return val, ok

	default:
		return nil, false
	}
}

// checkConstraint checks if a constraint is satisfied
func checkConstraint(ctx Context, operand string, lVal, rVal interface{}) bool {
	// Check for constraints not handled by this iterator.
	switch operand {
	case "unique":
		return true
	default:
		break
	}

	switch operand {
	case "=", "==", "is":
		return reflect.DeepEqual(lVal, rVal)
	case "!=", "not":
		return !reflect.DeepEqual(lVal, rVal)
	case "<", "<=", ">", ">=":
		return checkLexicalOrder(operand, lVal, rVal)
	case "version":
		return checkVersionConstraint(ctx, lVal, rVal)
	case "regexp":
		return checkRegexpConstraint(ctx, lVal, rVal)
	default:
		return false
	}
}

// checkLexicalOrder is used to check for lexical ordering
func checkLexicalOrder(op string, lVal, rVal interface{}) bool {
	// Ensure the values are strings
	lStr, ok := lVal.(string)
	if !ok {
		return false
	}
	rStr, ok := rVal.(string)
	if !ok {
		return false
	}

	switch op {
	case "<":
		return lStr < rStr
	case "<=":
		return lStr <= rStr
	case ">":
		return lStr > rStr
	case ">=":
		return lStr >= rStr
	default:
		return false
	}
}

// checkVersionConstraint is used to compare a version on the
// left hand side with a set of constraints on the right hand side
func checkVersionConstraint(ctx Context, lVal, rVal interface{}) bool {
	// Parse the version
	var versionStr string
	switch v := lVal.(type) {
	case string:
		versionStr = v
	case int:
		versionStr = fmt.Sprintf("%d", v)
	default:
		return false
	}

	// Parse the verison
	vers, err := version.NewVersion(versionStr)
	if err != nil {
		return false
	}

	// Constraint must be a string
	constraintStr, ok := rVal.(string)
	if !ok {
		return false
	}

	// Check the cache for a match
	cache := ctx.ConstraintCache()
	constraints := cache[constraintStr]

	// Parse the constraints
	if constraints == nil {
		constraints, err = version.NewConstraint(constraintStr)
		if err != nil {
			return false
		}
		cache[constraintStr] = constraints
	}

	// Check the constraints against the version
	return constraints.Check(vers)
}

// checkRegexpConstraint is used to compare a value on the
// left hand side with a regexp on the right hand side
func checkRegexpConstraint(ctx Context, lVal, rVal interface{}) bool {
	// Ensure left-hand is string
	lStr, ok := lVal.(string)
	if !ok {
		return false
	}

	// Regexp must be a string
	regexpStr, ok := rVal.(string)
	if !ok {
		return false
	}

	// Check the cache
	cache := ctx.RegexpCache()
	re := cache[regexpStr]

	// Parse the regexp
	if re == nil {
		var err error
		re, err = regexp.Compile(regexpStr)
		if err != nil {
			return false
		}
		cache[regexpStr] = re
	}

	// Look for a match
	return re.MatchString(lStr)
}
