package file_manager

import (
	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/ioworker"
	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/parser"
	"github.com/sirupsen/logrus"
)

type OpenFile struct {
	WaitingQueries []parser.Query
	Writter        int
	Readers        int
}

type FileManager struct {
	InReadQueries   *chan parser.Query
	InWriteQueries  *chan parser.Query
	OutReadQueries  chan parser.Query
	OutWriteQueries chan parser.Query
	FinishedQueries chan ioworker.Ack
	Dispatcher      map[string]*OpenFile
}

func NewFileManager(rq, wq *chan parser.Query, nWorkers int) *FileManager {

	f := &FileManager{
		InReadQueries:   rq,
		InWriteQueries:  wq,
		OutReadQueries:  make(chan parser.Query, nWorkers),
		OutWriteQueries: make(chan parser.Query, nWorkers),
		FinishedQueries: make(chan ioworker.Ack, nWorkers),
		Dispatcher:      make(map[string]*OpenFile),
	}

	for i := 0; i < nWorkers; i++ {
		go ioworker.Write(&f.OutWriteQueries, &f.FinishedQueries)
		go ioworker.Read(&f.OutReadQueries, &f.FinishedQueries)
	}

	return f
}

func (f *FileManager) Run(quit *chan bool) {
	for {
		select {
		case q := <-*f.InReadQueries:
			logrus.Info("proccesing new Read Query")
			q.Read.Initialize = true
			openFile, ok := f.Dispatcher[q.Read.AppId]
			if !ok {

				f.Dispatcher[q.Read.AppId] = &OpenFile{
					WaitingQueries: make([]parser.Query, 0),
					Readers:        1,
				}
				f.OutReadQueries <- q
				continue
			} else if len(openFile.WaitingQueries) > 0 || openFile.Writter > 0 {
				openFile.WaitingQueries = append(openFile.WaitingQueries, q)
				f.Dispatcher[q.Read.AppId] = openFile
			} else {
				openFile.Readers = openFile.Readers + 1
				f.Dispatcher[q.Read.AppId] = openFile
				f.OutReadQueries <- q
			}

		case q := <-*f.InWriteQueries:
			logrus.Info("proccesing new Write Query")
			q.Write.Initialize = true
			openFile, ok := f.Dispatcher[q.Write.AppId]
			if !ok {
				f.Dispatcher[q.Write.AppId] = &OpenFile{
					WaitingQueries: make([]parser.Query, 0),
					Writter:        1,
				}
				f.OutWriteQueries <- q
			} else {
				openFile.WaitingQueries = append(openFile.WaitingQueries, q)
				f.Dispatcher[q.Write.AppId] = openFile
			}

		case ack := <-f.FinishedQueries:
			openFile := f.Dispatcher[ack.AppId]
			if ack.Read {
				openFile.Readers = openFile.Readers - 1
				if openFile.Readers != 0 {
					f.Dispatcher[ack.AppId] = openFile
					continue
				}
				if len(openFile.WaitingQueries) == 0 {
					delete(f.Dispatcher, ack.AppId)
					continue
				}
				//conjetura : el proximo es escritor

				nextQuery := openFile.WaitingQueries[0]
				if !nextQuery.IsWriteQuery() {
					logrus.Info("houston tenemos un problema")
				}
				openFile.WaitingQueries = openFile.WaitingQueries[1:]
				openFile.Writter = openFile.Writter + 1
				f.Dispatcher[ack.AppId] = openFile
				f.OutWriteQueries <- nextQuery
			} else if ack.Write {
				if len(openFile.WaitingQueries) == 0 {
					delete(f.Dispatcher, ack.AppId)
					continue
				}
				nextQuery := openFile.WaitingQueries[0]
				if nextQuery.IsWriteQuery() {
					openFile.WaitingQueries = openFile.WaitingQueries[1:]
					f.Dispatcher[ack.AppId] = openFile
					f.OutWriteQueries <- nextQuery
				} else {
					openFile.Writter = openFile.Writter - 1
					var indice int
					for i, v := range openFile.WaitingQueries {
						indice = i
						if v.IsReadQuery() {
							select {
							case f.OutReadQueries <- v:
								openFile.Readers = openFile.Readers + 1
							default:
							}

						} else {
							indice -= 1
							break
						}
					}
					openFile.WaitingQueries = openFile.WaitingQueries[indice+1:]
					f.Dispatcher[ack.AppId] = openFile
				}
			}
		case q := <-*quit:
			if q {
				close(f.OutReadQueries)
				close(f.OutWriteQueries)
				close(f.FinishedQueries)
				logrus.Info("Closing File Manager")
				return
			}
		}
	}
}
