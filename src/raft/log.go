package raft

type entry struct {
	Command interface{}
	Term    int
	Index   int
}

type Log struct {
	Log    []entry
	Index0 int
}

func (log *Log) start() int {
	return log.Index0
}

func makeEmptyLog() Log {
	return Log{make([]entry, 1), 0}
}

func makeLog(entries []entry, index0 int) Log {

	// 第一个是空的
	log := Log{make([]entry, 1), index0}
	log.Log = append(log.Log, entries...)
	return log
}

func (l *Log) append(e entry) {
	l.Log = append(l.Log, e)
}

func (l *Log) cutEnd(index int) {
	l.Log = l.Log[0 : index-l.Index0]
}
func (l *Log) at(index int) *entry {
	return &l.Log[index-l.Index0]
}

func (l *Log) latestIndex() int {
	return l.Index0 + len(l.Log) - 1
}
func (l *Log) latestEntry() *entry {
	return &l.Log[len(l.Log)-1]
}

func (l *Log) slice(low, high int) []entry {
	return l.Log[low-l.Index0 : high+1-l.Index0]
}
