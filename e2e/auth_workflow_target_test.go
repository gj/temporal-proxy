package e2e

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/proxy/internal/api"
	"go.temporal.io/proxy/internal/dataplane/dataplanetest"
	authv1 "go.temporal.io/proxy/pkg/api/auth/v1"
)

// Admits every call and records the target of each.
type recordingAuthz struct {
	authv1.UnimplementedAuthServiceServer

	mu      sync.Mutex
	targets map[string]*authv1.Target // by full method
}

func (a *recordingAuthz) Auth(_ context.Context, req *authv1.AuthRequest) (*authv1.AuthResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.targets[req.GetTarget().GetFullName()] = req.GetTarget()

	return &authv1.AuthResponse{Decision: authv1.AuthResponse_DECISION_ALLOW}, nil
}

func (a *recordingAuthz) workflowServiceTarget(method string) *authv1.WorkflowServiceTarget {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.targets["/temporal.api.workflowservice.v1.WorkflowService/"+method].GetWorkflowService()
}

func TestExtensionServerReceivesWorkflowTarget(t *testing.T) {
	t.Parallel()

	authz := &recordingAuthz{targets: map[string]*authv1.Target{}}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	authv1.RegisterAuthServiceServer(srv, authz)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	f := dataplanetest.Start(t, dataplanetest.Config(dataplanetest.NewUpstream(t)),
		dataplanetest.WithAuth(api.NewAuth(cc, nil)))

	// The fake upstream implements neither method. The extension server is asked
	// before the upstream is, so the errors do not matter here.
	_, _ = f.Client().StartWorkflowExecution(f.Context(), &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    "default",
		WorkflowId:   "wf-1",
		WorkflowType: &commonpb.WorkflowType{Name: "Refund"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "tq-1"},
	}, grpc.WaitForReady(true))
	_, _ = f.Client().TerminateWorkflowExecution(f.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         "default",
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-1"},
	}, grpc.WaitForReady(true))

	workflows := func(op *authv1.WorkflowOperation) *authv1.WorkflowServiceTarget {
		return &authv1.WorkflowServiceTarget{Kind: &authv1.WorkflowServiceTarget_Workflows{
			Workflows: &authv1.WorkflowOperations{Operations: []*authv1.WorkflowOperation{op}},
		}}
	}

	wantStart := workflows(&authv1.WorkflowOperation{
		WorkflowId: "wf-1",
		Start:      &authv1.WorkflowStart{WorkflowType: "Refund", TaskQueue: "tq-1"},
	})
	gotStart := authz.workflowServiceTarget("StartWorkflowExecution")
	require.True(t, proto.Equal(wantStart, gotStart), "got %v", gotStart)

	wantTerminate := workflows(&authv1.WorkflowOperation{WorkflowId: "wf-1"})
	gotTerminate := authz.workflowServiceTarget("TerminateWorkflowExecution")
	require.True(t, proto.Equal(wantTerminate, gotTerminate), "got %v", gotTerminate)
}
