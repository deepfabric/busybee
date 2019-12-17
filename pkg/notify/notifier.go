package notify

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
)

// Notifier service notify
type Notifier interface {
	Notify(...metapb.Notify) error
}

// NewNotifier create a notifier
func NewNotifier() Notifier {
	return &localNotifier{}
}

type localNotifier struct {
}

func (n *localNotifier) Notify(notifies ...metapb.Notify) error {

	return nil
}
