/* See LICENSE file for copyright and license details. */

package mesh

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type MeSHRecord struct {
	MH      string
	MN      []string
	Entries map[string]bool
	UI      string
}

type MeSHRecordsMap map[string]*MeSHRecord

type MeSHNode struct {
	cont map[string]*MeSHNode
}

func NewNode(contents map[string]*MeSHNode) *MeSHNode {
	return &MeSHNode{cont: contents}
}

func (mn *MeSHNode) Add(nodepath []string) {
	var curn *MeSHNode
	var nn *MeSHNode
	var ok bool

	nn = mn
	for _, s := range nodepath {
		if curn, ok = nn.cont[s]; !ok {
			if nn == nil {
				nn = &MeSHNode{cont: make(map[string]*MeSHNode, 5)}
			}
			newNode := &MeSHNode{cont: make(map[string]*MeSHNode, 5)}
			nn.cont[s] = newNode
			nn = newNode
		} else {
			nn = curn
		}
	}
}

func (mn *MeSHNode) GetDict() map[string]*MeSHNode {
	return mn.cont
}

func (mn *MeSHNode) GetSamePrefix(prefix string) []string {
	var (
		curn, nn *MeSHNode
		ok       bool
		finres   []string
		path     string
	)
	splitpre := strings.Split(prefix, ".")

	nn = mn
	for _, s := range splitpre {
		if curn, ok = nn.cont[s]; !ok {
			return nil
		} else {
			path += s + "."
			nn = curn
		}
	}
	for k, _ := range nn.cont {
		var res []string

		curpath := path + k
		partres := getsuffices(curpath, res, nn.cont[k])
		finres = append(finres, append(partres, curpath)...)
		//fmt.Fprintf(os.Stderr, "GetSamePrefix curpath: %s partres: %#v, res: %#v\n", curpath, partres, res)
	}
	return finres
}

func getsuffices(path string, res []string, mn *MeSHNode) []string {
	for k, nn := range mn.cont {
		curpath := path + "." + k
		res = getsuffices(curpath, res, nn)
		res = append(res, curpath)
	}

	return res
}

func ParseMeSHTree(meshinput *bufio.Reader, meshnode MeSHNode) {
	lineno := 0
	for {
		line, err := meshinput.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error while reading MeSH tree file at line nr. %d: %v\n", lineno, err)
			os.Exit(1)
		}

		if line == "\n" {
			continue
		}

		splitl := strings.Split(line, ";")
		if len(splitl) < 2 {
			fmt.Printf("Error while reading MeSH tree file at line nr. %d. Split did not result in two values.\n", lineno)
		}
		trimmednodeid := strings.Trim(splitl[1], " \n")
		splitpath := strings.Split(trimmednodeid, ".")
		meshnode.Add(splitpath)
	}
}

func parseMeSH(meshinput bufio.Reader, meshchan chan *MeSHRecord, meshrecs MeSHRecordsMap) {
	var record *MeSHRecord
	var recordsstarted bool

	defer close(meshchan)

	quotrep := strings.NewReplacer("\"", "")
	lineno := 0

	for {
		line, err := meshinput.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error while reading obo file at line nr. %d: %v\n", lineno, err)
			os.Exit(1)
		}
		lineno++
		line = line[:len(line)-1] // chop \n
		if lineno%1000000 == 0 {
			fmt.Fprintf(os.Stderr, "Chopped line number: %d\n", lineno)
		}

		switch line {
		case "*NEWRECORD":
			recordsstarted = true
			if record != nil {
				meshchan <- record
			}

			record = &MeSHRecord{Entries: make(map[string]bool, 5)}
			continue
		case "\n":
			continue
		case "":
			continue
		default:
			if line[0] == '!' {
				continue
			}
		}

		if !recordsstarted {
			continue
		}

		splitline := strings.SplitN(line, " = ", 2)
		trimmedvalue := strings.Trim(splitline[1], " ")
		field := strings.Trim(splitline[0], " ")
		switch field {
		case "UI":
			record.UI = trimmedvalue
		case "MH":
			record.MH = trimmedvalue
		case "MN":
			record.MN = append(record.MN, trimmedvalue)
			meshrecs[trimmedvalue] = record
		case "ENTRY", "PRINT ENTRY":
			synline := strings.SplitN(trimmedvalue, "|", 2)
			synstr := synline[0]
			if strings.Contains(synstr, ", ") {
				parts := strings.SplitN(synstr, ", ", 2)
				synstr = parts[1] + " " + parts[0]
			}
			record.Entries[quotrep.Replace(synstr)] = true
		}
	}
	meshchan <- record
}

// Parses a MeSH into a slice of MeSHRecords and also fills a map to the
// records and returns it.
func ParseToSliceAndMap(meshinput bufio.Reader, meshrecords MeSHRecordsMap, mrslice []*MeSHRecord) ([]*MeSHRecord, MeSHRecordsMap) {
	meshchan := make(chan *MeSHRecord, 1000)

	go parseMeSH(meshinput, meshchan, meshrecords)
	for mr := range meshchan {
		mrslice = append(mrslice, mr)
	}

	return mrslice, meshrecords
}

// This function returns a channel on which pointers to the parsed
// MeSHRecords will be sent.
func ParseToChannel(meshinput bufio.Reader, meshchan chan *MeSHRecord) chan *MeSHRecord {
	meshrecmap := make(map[string]*MeSHRecord)
	go parseMeSH(meshinput, meshchan, meshrecmap)

	return meshchan
}

// This function returns a channel on which pointers to the parsed
// MeSHRecords will be sent. We also return the map to the MeSHRecords
// which can only be used after the channel has been closed (because
// this indicates that the parsing has been completed).
func ParseToChannelAndMap(meshinput bufio.Reader, meshchan chan *MeSHRecord, meshrecords MeSHRecordsMap) (chan *MeSHRecord, MeSHRecordsMap) {

	go parseMeSH(meshinput, meshchan, meshrecords)

	return meshchan, meshrecords
}
