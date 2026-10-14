package mocks

import (
	"github.com/stretchr/testify/mock"

	"context"

	"github.com/vladopajic/go-actor/actor"

	"link-society.com/flowg/internal/engines/lognotify"
	"link-society.com/flowg/internal/models"
)

// MockNotifier is a testify mock implementation of LogNotifier for use in tests.
type MockNotifier struct {
	mock.Mock
}

var _ lognotify.LogNotifier = (*MockNotifier)(nil)

// NewMockNotifier returns a LogNotifier whose calls can be stubbed and asserted.
func NewMockNotifier() lognotify.LogNotifier {
	return &MockNotifier{}
}

func (m *MockNotifier) Subscribe(ctx context.Context, stream string) (actor.MailboxReceiver[lognotify.LogMessage], error) {
	args := m.Called(ctx, stream)
	return args.Get(0).(actor.MailboxReceiver[lognotify.LogMessage]), args.Error(1)
}

func (m *MockNotifier) Notify(ctx context.Context, stream string, logKey string, logRecord models.LogRecord) error {
	args := m.Called(ctx, stream, logKey, logRecord)
	return args.Error(0)
}

func (m *MockNotifier) CloseStream(ctx context.Context, stream, reason string) error {
	args := m.Called(ctx, stream, reason)
	return args.Error(0)
}

func (m *MockNotifier) ReopenStream(ctx context.Context, stream string) error {
	args := m.Called(ctx, stream)
	return args.Error(0)
}
