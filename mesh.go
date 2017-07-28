/* See LICENSE file for copyright and license details. */

package mesh

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
)

// MeSHParser can be used to parse the MesH records. To do that it
// exposes several methods. Please note that only one method can be called
// for each parser. That means if you want to parse the data in two
// different ways (using two of the different methods that the MeSHParser
// exposes) you will have to create a MeSHParser for each method to call.
type MeSHParser struct {
	meshinput   bufio.Reader
	quotrep     *strings.Replacer
	meshrecords MeSHRecordsMap
}

// MeSHTreeParser parses the MeSHTree into a tree of nodes.
type MeSHTreeParser struct {
	meshinput bufio.Reader
}

type MeSHRecord struct {
	MH      string
	MN      []string
	Entries map[string]bool
	UI      string
	MS      string
}

type MeSHRecordsMap map[string]*MeSHRecord

type MeSHNode struct {
	cont map[string]*MeSHNode
}

func NewNode(contents map[string]*MeSHNode) *MeSHNode {
	return &MeSHNode{cont: contents}
}

// NewMeSHParser returns a new MeSHParser that can be used to parse MeSH.
func NewMeSHParser(r bufio.Reader) *MeSHParser {
	mp := &MeSHParser{
		meshinput:   r,
		meshrecords: make(map[string]*MeSHRecord, 50000),
		quotrep:     strings.NewReplacer("\"", ""),
	}

	return mp
}

// NewMeSHTreeParser returns a new MeSHTreeParser that can be used to
// parse the MeSH tree.
func NewMeSHTreeParser(r bufio.Reader) *MeSHTreeParser {
	mtp := &MeSHTreeParser{
		meshinput: r,
	}

	return mtp
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

func (mtp *MeSHTreeParser) ParseMeSHTree(meshnode MeSHNode) {
	lineno := 0
	for {
		line, err := mtp.meshinput.ReadString('\n')
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

func (mp *MeSHParser) parseMeSH(meshchan chan *MeSHRecord) {
	var (
		record         *MeSHRecord
		recordsstarted bool
		prevField      string
		fieldBuffer    bytes.Buffer
	)

	defer close(meshchan)

	lineno := 0

	for {
		line, err := mp.meshinput.ReadString('\n')
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
				mp.writeRecordField(record, prevField, fieldBuffer)
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
		if len(splitline) < 2 {
			fieldBuffer.WriteString(strings.TrimSpace(splitline[0]))
			continue
		} else {
			if fieldBuffer.Len() > 0 {
				mp.writeRecordField(record, prevField, fieldBuffer)
				fieldBuffer.Reset()
			}
			fieldBuffer.WriteString(strings.TrimSpace(splitline[1]))
		}

		prevField = strings.Trim(splitline[0], " ")
	}

	mp.writeRecordField(record, prevField, fieldBuffer)
	fieldBuffer.Reset()
	meshchan <- record
}

func (mp *MeSHParser) writeRecordField(record *MeSHRecord, fieldName string, buf bytes.Buffer) {
	value := buf.String()
	switch fieldName {
	case "UI":
		record.UI = value
	case "MH":
		record.MH = value
	case "MS":
		record.MS = value
	case "MN":
		record.MN = append(record.MN, value)
		mp.meshrecords[value] = record
	case "ENTRY", "PRINT ENTRY":
		synline := strings.SplitN(value, "|", 2)
		synstr := synline[0]
		if strings.Contains(synstr, ", ") {
			parts := strings.SplitN(synstr, ", ", 2)
			synstr = parts[1] + " " + parts[0]
		}
		record.Entries[mp.quotrep.Replace(synstr)] = true
	}
}

// Parses a MeSH into a slice of MeSHRecords and also fills a map to the
// records and returns it.
func (mp *MeSHParser) ParseToSliceAndMap() ([]*MeSHRecord, MeSHRecordsMap) {
	meshchan := make(chan *MeSHRecord, 1000)
	mrslice := make([]*MeSHRecord, 0, 50000)

	go mp.parseMeSH(meshchan)
	for mr := range meshchan {
		mrslice = append(mrslice, mr)
	}

	return mrslice, mp.meshrecords
}

// This function returns a channel on which pointers to the parsed
// MeSHRecords will be sent.
func (mp *MeSHParser) ParseToChannel(meshchan chan *MeSHRecord) chan *MeSHRecord {
	go mp.parseMeSH(meshchan)

	return meshchan
}

// This function returns a channel on which pointers to the parsed
// MeSHRecords will be sent. We also return the map to the MeSHRecords
// which can only be used after the channel has been closed (because
// this indicates that the parsing has been completed).
func (mp *MeSHParser) ParseToChannelAndMap(meshchan chan *MeSHRecord) (chan *MeSHRecord, MeSHRecordsMap) {

	go mp.parseMeSH(meshchan)

	return meshchan, mp.meshrecords
}
