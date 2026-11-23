package protoutil_test

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"go.temporal.io/proxy/internal/protoutil"
	"go.temporal.io/proxy/internal/transport/meta"
	authv1 "go.temporal.io/proxy/pkg/api/auth/v1"
)

const (
	wfService    = "temporal.api.workflowservice.v1.WorkflowService"
	startMethod  = "/" + wfService + "/StartWorkflowExecution"
	systemMethod = "/" + wfService + "/GetSystemInfo"
)

type (
	errFiles struct{ err error }
	errTypes struct{ err error }
)

func TestExtractorNamespace(t *testing.T) {
	t.Parallel()

	start := mustMarshal(t, &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns-1"})
	system := mustMarshal(t, &workflowservice.GetSystemInfoRequest{})

	tests := []struct {
		name    string
		method  string
		payload []byte
		want    string
	}{
		{name: "namespaced request", method: startMethod, payload: start, want: "ns-1"},
		{name: "system request has no namespace", method: systemMethod, payload: system, want: ""},
		{name: "unknown service", method: "/nope.v1.Service/Method", payload: start, want: ""},
		{name: "name resolves to a non-service", method: "/temporal.api.workflowservice.v1.StartWorkflowExecutionRequest/Method", payload: start, want: ""},
		{name: "unknown method on real service", method: "/" + wfService + "/NoSuchMethod", payload: start, want: ""},
		{name: "malformed method name", method: "no-slashes", payload: start, want: ""},
		{name: "garbage payload", method: startMethod, payload: []byte{0xff, 0xff, 0xff}, want: ""},
		{name: "empty namespace", method: startMethod, payload: mustMarshal(t, &workflowservice.StartWorkflowExecutionRequest{}), want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
			require.Equal(t, tt.want, ex.Namespace(tt.method, tt.payload))
		})
	}
}

func TestExtractorCaching(t *testing.T) {
	t.Parallel()

	start := mustMarshal(t, &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns-1"})

	t.Run("resolved type is reused", func(t *testing.T) {
		t.Parallel()
		ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
		require.Equal(t, "ns-1", ex.Namespace(startMethod, start))
		require.Equal(t, "ns-1", ex.Namespace(startMethod, start), "second lookup uses the cached type")
	})

	t.Run("unknown method stays empty on repeat", func(t *testing.T) {
		t.Parallel()
		ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
		require.Equal(t, "", ex.Namespace("/nope.v1.Service/Method", start))
		require.Equal(t, "", ex.Namespace("/nope.v1.Service/Method", start), "a repeated miss must not panic")
	})

	t.Run("only resolved methods are cached", func(t *testing.T) {
		t.Parallel()

		// The gateway calls this before authentication with whatever path the
		// caller sent, so a cached miss would let any caller grow the cache.
		files := &countingFiles{Files: protoregistry.GlobalFiles}
		ex := protoutil.NewExtractor(files, protoregistry.GlobalTypes)

		ex.Namespace(startMethod, start)
		ex.Namespace(startMethod, start)
		require.Equal(t, int64(1), files.n.Load(), "a resolved method is looked up once")

		junk := "/" + wfService + "/NoSuchMethod"
		ex.Namespace(junk, start)
		ex.Namespace(junk, start)
		require.Equal(t, int64(3), files.n.Load(), "a miss is resolved again, so it was not retained")
	})
}

// countingFiles counts descriptor lookups, so a test can tell a cached
// resolution from a repeated one.
type countingFiles struct {
	protoutil.Files
	n atomic.Int64
}

func (f *countingFiles) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	f.n.Add(1)
	return f.Files.FindDescriptorByName(name)
}

func TestExtractorConsultsInjectedSources(t *testing.T) {
	t.Parallel()

	start := mustMarshal(t, &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns-1"})

	t.Run("files error yields empty", func(t *testing.T) {
		t.Parallel()
		ex := protoutil.NewExtractor(errFiles{err: errors.New("boom")}, protoregistry.GlobalTypes)
		require.Equal(t, "", ex.Namespace(startMethod, start))
	})

	t.Run("types error yields empty", func(t *testing.T) {
		t.Parallel()
		// Real files resolve the service and method; the injected types then
		// fails, so extraction stops there. This proves types is consulted.
		ex := protoutil.NewExtractor(protoregistry.GlobalFiles, errTypes{err: errors.New("boom")})
		require.Equal(t, "", ex.Namespace(startMethod, start))
	})
}

func (f errFiles) FindDescriptorByName(protoreflect.FullName) (protoreflect.Descriptor, error) {
	return nil, f.err
}

func (t errTypes) FindMessageByName(protoreflect.FullName) (protoreflect.MessageType, error) {
	return nil, t.err
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	require.NoError(t, err)
	return b
}

func TestExtractorTarget(t *testing.T) {
	t.Parallel()

	const ns = "ns-1"
	execution := &commonpb.WorkflowExecution{WorkflowId: "wf-1"}
	workflowType := &commonpb.WorkflowType{Name: "Refund"}
	taskQueue := &taskqueuepb.TaskQueue{Name: "tq-1"}

	start := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: ns, WorkflowId: "wf-1", WorkflowType: workflowType, TaskQueue: taskQueue,
	}
	schedule := &schedulepb.Schedule{Action: &schedulepb.ScheduleAction{
		Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
			WorkflowId: "wf-1", WorkflowType: workflowType, TaskQueue: taskQueue,
		}},
	}}

	type target = authv1.WorkflowServiceTarget
	onWF1 := &authv1.WorkflowOperation{WorkflowId: "wf-1"}
	onWF2 := &authv1.WorkflowOperation{WorkflowId: "wf-2"}
	startWF1 := &authv1.WorkflowOperation{
		WorkflowId: "wf-1",
		Start:      &authv1.WorkflowStart{WorkflowType: "Refund", TaskQueue: "tq-1"},
	}
	workflows := func(ops ...*authv1.WorkflowOperation) *target {
		return &target{Kind: &authv1.WorkflowServiceTarget_Workflows{Workflows: &authv1.WorkflowOperations{Operations: ops}}}
	}
	scheduleTarget := func(action *authv1.WorkflowOperation) *target {
		return &target{Kind: &authv1.WorkflowServiceTarget_Schedule{
			Schedule: &authv1.ScheduleOperation{ScheduleId: "sched-1", Action: action},
		}}
	}
	queue := &target{Kind: &authv1.WorkflowServiceTarget_TaskQueue{TaskQueue: "tq-1"}}
	query := &target{Kind: &authv1.WorkflowServiceTarget_VisibilityQuery{VisibilityQuery: "WorkflowType = 'Refund'"}}
	none := &target{}

	tests := []struct {
		name    string
		method  string
		request proto.Message
		want    *target
	}{
		{"start", "StartWorkflowExecution", start, workflows(startWF1)},
		{
			"signal-with-start", "SignalWithStartWorkflowExecution",
			&workflowservice.SignalWithStartWorkflowExecutionRequest{
				Namespace: ns, WorkflowId: "wf-1", WorkflowType: workflowType, TaskQueue: taskQueue,
			},
			workflows(startWF1),
		},

		{"signal", "SignalWorkflowExecution", &workflowservice.SignalWorkflowExecutionRequest{Namespace: ns, WorkflowExecution: execution}, workflows(onWF1)},
		{"terminate", "TerminateWorkflowExecution", &workflowservice.TerminateWorkflowExecutionRequest{Namespace: ns, WorkflowExecution: execution}, workflows(onWF1)},
		{"cancel", "RequestCancelWorkflowExecution", &workflowservice.RequestCancelWorkflowExecutionRequest{Namespace: ns, WorkflowExecution: execution}, workflows(onWF1)},
		{"reset", "ResetWorkflowExecution", &workflowservice.ResetWorkflowExecutionRequest{Namespace: ns, WorkflowExecution: execution}, workflows(onWF1)},
		{"delete", "DeleteWorkflowExecution", &workflowservice.DeleteWorkflowExecutionRequest{Namespace: ns, WorkflowExecution: execution}, workflows(onWF1)},
		{"update", "UpdateWorkflowExecution", &workflowservice.UpdateWorkflowExecutionRequest{Namespace: ns, WorkflowExecution: execution}, workflows(onWF1)},
		{"describe", "DescribeWorkflowExecution", &workflowservice.DescribeWorkflowExecutionRequest{Namespace: ns, Execution: execution}, workflows(onWF1)},
		{"history", "GetWorkflowExecutionHistory", &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: ns, Execution: execution}, workflows(onWF1)},
		{"query", "QueryWorkflow", &workflowservice.QueryWorkflowRequest{Namespace: ns, Execution: execution}, workflows(onWF1)},

		{
			"multi-operation: start wf-1, update wf-2", "ExecuteMultiOperation",
			&workflowservice.ExecuteMultiOperationRequest{
				Namespace: ns,
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					{Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{StartWorkflow: start}},
					{Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
						UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionRequest{
							WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-2"},
						},
					}},
				},
			},
			workflows(startWF1, onWF2),
		},

		{
			"batch over listed workflows, in either list field", "StartBatchOperation",
			&workflowservice.StartBatchOperationRequest{
				Namespace:        ns,
				Executions:       []*commonpb.WorkflowExecution{{WorkflowId: "wf-1"}}, //nolint:staticcheck // deprecated, still accepted
				TargetExecutions: []*commonpb.Execution{{Type: enumspb.EXECUTION_TYPE_WORKFLOW, BusinessId: "wf-2"}},
			},
			workflows(onWF1, onWF2),
		},
		{
			"batch over a visibility query", "StartBatchOperation",
			&workflowservice.StartBatchOperationRequest{Namespace: ns, VisibilityQuery: "WorkflowType = 'Refund'"},
			query,
		},
		{
			"batch that targets a non-workflow", "StartBatchOperation",
			&workflowservice.StartBatchOperationRequest{
				Namespace: ns,
				TargetExecutions: []*commonpb.Execution{
					{Type: enumspb.EXECUTION_TYPE_WORKFLOW, BusinessId: "wf-1"},
					{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: "activity-1"},
				},
			},
			none,
		},

		{"create schedule", "CreateSchedule", &workflowservice.CreateScheduleRequest{Namespace: ns, ScheduleId: "sched-1", Schedule: schedule}, scheduleTarget(startWF1)},
		{"update schedule", "UpdateSchedule", &workflowservice.UpdateScheduleRequest{Namespace: ns, ScheduleId: "sched-1", Schedule: schedule}, scheduleTarget(startWF1)},
		{"patch schedule", "PatchSchedule", &workflowservice.PatchScheduleRequest{Namespace: ns, ScheduleId: "sched-1"}, scheduleTarget(nil)},
		{"delete schedule", "DeleteSchedule", &workflowservice.DeleteScheduleRequest{Namespace: ns, ScheduleId: "sched-1"}, scheduleTarget(nil)},

		{"poll", "PollWorkflowTaskQueue", &workflowservice.PollWorkflowTaskQueueRequest{Namespace: ns, TaskQueue: taskQueue}, queue},
		{"versioning rules", "UpdateWorkerVersioningRules", &workflowservice.UpdateWorkerVersioningRulesRequest{Namespace: ns, TaskQueue: "tq-1"}, queue},
		{"task queue config", "UpdateTaskQueueConfig", &workflowservice.UpdateTaskQueueConfigRequest{Namespace: ns, TaskQueue: "tq-1"}, queue},

		{"list", "ListWorkflowExecutions", &workflowservice.ListWorkflowExecutionsRequest{Namespace: ns}, none},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fullMethod := "/" + wfService + "/" + tt.method
			ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
			got := ex.Target(fullMethod, mustMarshal(t, tt.request))

			require.Equal(t, fullMethod, got.FullName)
			require.Equal(t, ns, got.Namespace)
			require.True(t, proto.Equal(tt.want, got.WorkflowService), "got %v", got.WorkflowService)
		})
	}

	t.Run("a payload that cannot be decoded gives only the method", func(t *testing.T) {
		t.Parallel()

		ex := protoutil.NewExtractor(protoregistry.GlobalFiles, protoregistry.GlobalTypes)
		require.Equal(t, meta.Target{FullName: startMethod}, ex.Target(startMethod, []byte{0xff, 0xff, 0xff}))
	})
}
