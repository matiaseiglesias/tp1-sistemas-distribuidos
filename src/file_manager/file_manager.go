package file_manager

import (
	"fmt"

	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/ioworker"
	"github.com/matiaseiglesias/tp1-sistemas-distribuidos/src/parser"
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

func NewFileManager(rq, wq *chan parser.Query) *FileManager {

	f := &FileManager{
		InReadQueries:   rq,
		InWriteQueries:  wq,
		OutReadQueries:  make(chan parser.Query, 100),
		OutWriteQueries: make(chan parser.Query, 100),
		FinishedQueries: make(chan ioworker.Ack, 100),
		Dispatcher:      make(map[string]*OpenFile),
	}

	go ioworker.Write(&f.OutWriteQueries, &f.FinishedQueries)
	go ioworker.Write(&f.OutWriteQueries, &f.FinishedQueries)
	go ioworker.Read(&f.OutReadQueries, &f.FinishedQueries)
	go ioworker.Read(&f.OutReadQueries, &f.FinishedQueries)

	//init ioworkers

	return f
}

func (f *FileManager) Run(quit *chan bool) {
	for {
		select {
		case q := <-*f.InReadQueries:
			fmt.Println("llego una lectura")
			q.Read.Initialize = true
			openFile, ok := f.Dispatcher[q.Read.AppId]
			if !ok {
				fmt.Println("Leo archivo que no estaba cacheado")
				f.Dispatcher[q.Read.AppId] = &OpenFile{
					WaitingQueries: make([]parser.Query, 0),
					Readers:        1,
				}
				f.OutReadQueries <- q
				//fmt.Println("lectura 1 lista: ", f.Dispatcher[q.Read.AppId].WaitingQueries)
				continue
			} else if len(openFile.WaitingQueries) > 0 || openFile.Writter > 0 {
				fmt.Println("encolo lectura, escritores esperando")
				openFile.WaitingQueries = append(openFile.WaitingQueries, q)
				f.Dispatcher[q.Read.AppId] = openFile
			} else {
				fmt.Println("Me pongo a leer")
				openFile.Readers = openFile.Readers + 1
				f.Dispatcher[q.Read.AppId] = openFile
				f.OutReadQueries <- q
			}
			//fmt.Println("lectura lista: ", f.Dispatcher[q.Read.AppId].WaitingQueries)

		case q := <-*f.InWriteQueries:
			fmt.Println("llego una escritura")
			q.Write.Initialize = true
			openFile, ok := f.Dispatcher[q.Write.AppId]
			if !ok {
				fmt.Println("Escribo archivo que no estaba cacheado")
				f.Dispatcher[q.Write.AppId] = &OpenFile{
					WaitingQueries: make([]parser.Query, 0),
					Writter:        1,
				}
				f.OutWriteQueries <- q
			} else {
				fmt.Println("encolo escritura")
				openFile.WaitingQueries = append(openFile.WaitingQueries, q)
				fmt.Println("voy a escribir: ", q)
				f.Dispatcher[q.Write.AppId] = openFile
				fmt.Println("bye")
			}
			//fmt.Println("escritura lista: ", f.Dispatcher[q.Write.AppId].WaitingQueries)

		case ack := <-f.FinishedQueries:
			fmt.Println("LLEGO UN ACK DE LA APP", ack.AppId)
			openFile := f.Dispatcher[ack.AppId]
			if ack.Read {
				fmt.Println("llego un ack lectura")
				fmt.Println("cantidad de lectores, ", openFile.Readers)
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
					fmt.Println("Heueston tenemos problemas")
					fmt.Println("Query conflictiva: ", nextQuery)
				}
				openFile.WaitingQueries = openFile.WaitingQueries[1:]
				openFile.Writter = openFile.Writter + 1
				f.Dispatcher[ack.AppId] = openFile
				f.OutWriteQueries <- nextQuery
			} else if ack.Write {
				fmt.Println("llego un ack escritura")
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
						fmt.Println("vacio cola de espera")
						if v.IsReadQuery() {
							fmt.Println("query: ", i, v)
							select {
							case f.OutReadQueries <- v:
								openFile.Readers = openFile.Readers + 1
							default:

								fmt.Println("Channel full. Discarding value")
							}

						} else {
							fmt.Println("encontre una query escritora, ", v)
							//openFile.WaitingQueries = openFile.WaitingQueries[i:]
							//f.Dispatcher[ack.AppId] = openFile
							fmt.Println("cantidad de escriores for", openFile.Writter)
							indice -= 1
							break
						}
					}
					openFile.WaitingQueries = openFile.WaitingQueries[indice+1:]
					f.Dispatcher[ack.AppId] = openFile
				}
				fmt.Println("cantidad de escritores", openFile.Writter)
			}
			//fmt.Println("ack lista: ", f.Dispatcher[ack.AppId].WaitingQueries)
		case q := <-*quit:
			if q {
				close(f.OutReadQueries)
				close(f.OutWriteQueries)
				close(f.FinishedQueries)
				fmt.Println("SE CIERRA EL FILE MANAGER")
				return
			}
		}
	}
}
