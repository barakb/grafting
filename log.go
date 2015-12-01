package go_rafting

type Log interface {
	Length() int
	Term(index int) (uint64, error)
}
