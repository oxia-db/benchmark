package sequence

var _ Generator = &order{}

type order struct {
	sequence uint64
}

func (o *order) Next() uint64 {
	sequence := o.sequence
	o.sequence = o.sequence + 1
	return sequence
}

func newOrder() Generator {
	return &order{
		sequence: 0,
	}
}
