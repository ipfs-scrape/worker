package ipfs

import "fmt"

type Metadata struct {
	ID          string `json:"ID"`
	CID         string `json:"CID"`
	Image       string `json:"Image"`
	Description string `json:"Description"`
	Name        string `json:"Name"`
}

func GenerateIDFromCID(cid string) string {
	return fmt.Sprintf("d-%s", cid)

}
