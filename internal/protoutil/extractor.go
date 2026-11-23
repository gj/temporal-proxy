package protoutil

import (
	"strings"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.temporal.io/proxy/internal/transport/meta"
	authv1 "go.temporal.io/proxy/pkg/api/auth/v1"
)

type (
	// Extractor reads the namespace from an encoded gRPC request. It resolves
	// the concrete request message type for a full method name from the
	// descriptor and type registries it was given, decodes the payload into it,
	// and reads the namespace via the generated accessor. Resolved types are
	// cached per method, so repeat calls for the same method skip the lookup.
	// Construct one with NewExtractor; an Extractor is safe for concurrent use.
	Extractor struct {
		msgs  sync.Map // fullMethod -> protoreflect.MessageType, resolved methods only
		files Files
		types Types
	}

	// Files resolves a descriptor by its fully qualified name. *protoregistry.Files,
	// and in particular protoregistry.GlobalFiles, implements it.
	Files interface {
		FindDescriptorByName(protoreflect.FullName) (protoreflect.Descriptor, error)
	}

	// Types resolves a message type by its fully qualified name. *protoregistry.Types,
	// and in particular protoregistry.GlobalTypes, implements it.
	Types interface {
		FindMessageByName(protoreflect.FullName) (protoreflect.MessageType, error)
	}

	// namespacer is implemented by the generated request types that carry a
	// namespace. Requests without one (e.g. GetSystemInfo) do not implement it,
	// so they yield an empty namespace.
	namespacer interface {
		GetNamespace() string
	}

	// Implemented by a start request, a signal-with-start request, and the
	// workflow a schedule's action starts.
	workflowStarter interface {
		GetWorkflowId() string
		GetWorkflowType() *commonpb.WorkflowType
		GetTaskQueue() *taskqueuepb.TaskQueue
	}
)

const workflowServicePrefix = "/temporal.api.workflowservice.v1.WorkflowService/"

// NewExtractor returns an Extractor that resolves request types using files and
// types. Production callers pass protoregistry.GlobalFiles and
// protoregistry.GlobalTypes; tests may pass fakes. Resolution only succeeds for
// message types registered in the given registries, so the relevant service
// packages (e.g. go.temporal.io/api/workflowservice/v1) must be imported into
// the binary.
func NewExtractor(files Files, types Types) *Extractor {
	return &Extractor{
		files: files,
		types: types,
	}
}

// Namespace returns the namespace carried by an encoded request for fullMethod,
// or the empty string when the method is unknown, the request type carries no
// namespace, or the payload cannot be decoded.
func (e *Extractor) Namespace(fullMethod string, payload []byte) string {
	return e.Target(fullMethod, payload).Namespace
}

// Target returns what an encoded request for fullMethod is addressing. Only
// FullName is set when the method is unknown or the payload cannot be decoded.
func (e *Extractor) Target(fullMethod string, payload []byte) meta.Target {
	target := meta.Target{FullName: fullMethod}

	mt := e.messageType(fullMethod)
	if mt == nil {
		return target
	}

	msg := mt.New().Interface()
	if err := proto.Unmarshal(payload, msg); err != nil {
		return target
	}

	if m, ok := msg.(namespacer); ok {
		target.Namespace = m.GetNamespace()
	}
	if strings.HasPrefix(fullMethod, workflowServicePrefix) {
		target.WorkflowService = workflowServiceTarget(msg)
	}

	return target
}

func workflowServiceTarget(msg proto.Message) *authv1.WorkflowServiceTarget {
	switch m := msg.(type) {
	case *workflowservice.ExecuteMultiOperationRequest:
		var ops []*authv1.WorkflowOperation
		for _, op := range m.GetOperations() {
			if start := op.GetStartWorkflow(); start != nil {
				ops = append(ops, startOperation(start))
			}
			if update := op.GetUpdateWorkflow(); update != nil {
				ops = append(ops, &authv1.WorkflowOperation{WorkflowId: update.GetWorkflowExecution().GetWorkflowId()})
			}
		}

		return workflowsTarget(ops...)

	case *workflowservice.StartBatchOperationRequest:
		if query := m.GetVisibilityQuery(); query != "" {
			return &authv1.WorkflowServiceTarget{Kind: &authv1.WorkflowServiceTarget_VisibilityQuery{VisibilityQuery: query}}
		}

		var ops []*authv1.WorkflowOperation
		for _, e := range m.GetExecutions() { //nolint:staticcheck // deprecated, still accepted
			ops = append(ops, &authv1.WorkflowOperation{WorkflowId: e.GetWorkflowId()})
		}
		for _, e := range m.GetTargetExecutions() {
			// A partial list would read as the full one.
			if e.GetType() != enumspb.EXECUTION_TYPE_WORKFLOW {
				return &authv1.WorkflowServiceTarget{}
			}
			ops = append(ops, &authv1.WorkflowOperation{WorkflowId: e.GetBusinessId()})
		}

		return workflowsTarget(ops...)

	case interface{ GetScheduleId() string }:
		schedule := &authv1.ScheduleOperation{ScheduleId: m.GetScheduleId()}
		if s, ok := msg.(interface{ GetSchedule() *schedulepb.Schedule }); ok {
			if start := s.GetSchedule().GetAction().GetStartWorkflow(); start != nil {
				schedule.Action = startOperation(start)
			}
		}

		return &authv1.WorkflowServiceTarget{Kind: &authv1.WorkflowServiceTarget_Schedule{Schedule: schedule}}

	case workflowStarter:
		return workflowsTarget(startOperation(m))

	// A request on an existing workflow has the workflow ID in one of three
	// fields.
	case interface{ GetWorkflowId() string }:
		return workflowsTarget(&authv1.WorkflowOperation{WorkflowId: m.GetWorkflowId()})
	case interface {
		GetWorkflowExecution() *commonpb.WorkflowExecution
	}:
		return workflowsTarget(&authv1.WorkflowOperation{WorkflowId: m.GetWorkflowExecution().GetWorkflowId()})
	case interface {
		GetExecution() *commonpb.WorkflowExecution
	}:
		return workflowsTarget(&authv1.WorkflowOperation{WorkflowId: m.GetExecution().GetWorkflowId()})

	// A request about a task queue has it as a message or as a plain string.
	case interface{ GetTaskQueue() *taskqueuepb.TaskQueue }:
		return &authv1.WorkflowServiceTarget{Kind: &authv1.WorkflowServiceTarget_TaskQueue{TaskQueue: m.GetTaskQueue().GetName()}}
	case interface{ GetTaskQueue() string }:
		return &authv1.WorkflowServiceTarget{Kind: &authv1.WorkflowServiceTarget_TaskQueue{TaskQueue: m.GetTaskQueue()}}
	}

	return &authv1.WorkflowServiceTarget{}
}

func workflowsTarget(ops ...*authv1.WorkflowOperation) *authv1.WorkflowServiceTarget {
	return &authv1.WorkflowServiceTarget{
		Kind: &authv1.WorkflowServiceTarget_Workflows{Workflows: &authv1.WorkflowOperations{Operations: ops}},
	}
}

func startOperation(s workflowStarter) *authv1.WorkflowOperation {
	return &authv1.WorkflowOperation{
		WorkflowId: s.GetWorkflowId(),
		Start: &authv1.WorkflowStart{
			WorkflowType: s.GetWorkflowType().GetName(),
			TaskQueue:    s.GetTaskQueue().GetName(),
		},
	}
}

// messageType returns the request message type for fullMethod, or nil when it
// cannot be resolved. A resolved type is cached, so the cache holds at most one
// entry per method in the registries. A miss is not: the gateway calls this
// before authentication with whatever path the caller sent, so caching misses
// would let any caller grow the cache, and the process, without bound.
func (e *Extractor) messageType(fullMethod string) protoreflect.MessageType {
	if v, ok := e.msgs.Load(fullMethod); ok {
		return v.(protoreflect.MessageType)
	}

	mt := e.resolveInputType(fullMethod)
	if mt == nil {
		return nil
	}

	actual, _ := e.msgs.LoadOrStore(fullMethod, mt)
	return actual.(protoreflect.MessageType)
}

// resolveInputType looks up the request message type for a "/pkg.Service/Method"
// name via the extractor's Files and Types, returning nil when the name is
// malformed or the service, method, or message type is not resolvable.
func (e *Extractor) resolveInputType(fullMethod string) protoreflect.MessageType {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	slash := strings.LastIndex(trimmed, "/")
	if slash < 0 {
		return nil
	}

	service := trimmed[:slash]
	method := trimmed[slash+1:]

	d, err := e.files.FindDescriptorByName(protoreflect.FullName(service))
	if err != nil {
		return nil
	}

	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil
	}

	md := sd.Methods().ByName(protoreflect.Name(method))
	if md == nil {
		return nil
	}

	mt, err := e.types.FindMessageByName(md.Input().FullName())
	if err != nil {
		return nil
	}

	return mt
}
