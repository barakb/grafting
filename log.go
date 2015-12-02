package go_rafting

type Log interface {
	Length() int
	Term(index int) Term
	Slice(from int, to int) []interface{}
}
