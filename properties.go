package mqttv5

import (
	"errors"
	"io"
)

// PropertyID represents an MQTT v5.0 property identifier.
type PropertyID byte

// Property identifiers as defined in MQTT v5.0 specification.
const (
	PropPayloadFormatIndicator   PropertyID = 0x01
	PropMessageExpiryInterval    PropertyID = 0x02
	PropContentType              PropertyID = 0x03
	PropResponseTopic            PropertyID = 0x08
	PropCorrelationData          PropertyID = 0x09
	PropSubscriptionIdentifier   PropertyID = 0x0B
	PropSessionExpiryInterval    PropertyID = 0x11
	PropAssignedClientIdentifier PropertyID = 0x12
	PropServerKeepAlive          PropertyID = 0x13
	PropAuthenticationMethod     PropertyID = 0x15
	PropAuthenticationData       PropertyID = 0x16
	PropRequestProblemInfo       PropertyID = 0x17
	PropWillDelayInterval        PropertyID = 0x18
	PropRequestResponseInfo      PropertyID = 0x19
	PropResponseInformation      PropertyID = 0x1A
	PropServerReference          PropertyID = 0x1C
	PropReasonString             PropertyID = 0x1F
	PropReceiveMaximum           PropertyID = 0x21
	PropTopicAliasMaximum        PropertyID = 0x22
	PropTopicAlias               PropertyID = 0x23
	PropMaximumQoS               PropertyID = 0x24
	PropRetainAvailable          PropertyID = 0x25
	PropUserProperty             PropertyID = 0x26
	PropMaximumPacketSize        PropertyID = 0x27
	PropWildcardSubAvailable     PropertyID = 0x28
	PropSubscriptionIDAvailable  PropertyID = 0x29
	PropSharedSubAvailable       PropertyID = 0x2A
)

// PropertyType represents the data type of a property value.
type PropertyType byte

const (
	PropTypeByte        PropertyType = 0 // Single byte
	PropTypeTwoByteInt  PropertyType = 1 // Two byte integer (uint16)
	PropTypeFourByteInt PropertyType = 2 // Four byte integer (uint32)
	PropTypeVarInt      PropertyType = 3 // Variable byte integer
	PropTypeString      PropertyType = 4 // UTF-8 encoded string
	PropTypeBinary      PropertyType = 5 // Binary data
	PropTypeStringPair  PropertyType = 6 // UTF-8 string pair
)

// propertyTypeMap maps property IDs to their data types.
var propertyTypeMap = map[PropertyID]PropertyType{
	PropPayloadFormatIndicator:   PropTypeByte,
	PropMessageExpiryInterval:    PropTypeFourByteInt,
	PropContentType:              PropTypeString,
	PropResponseTopic:            PropTypeString,
	PropCorrelationData:          PropTypeBinary,
	PropSubscriptionIdentifier:   PropTypeVarInt,
	PropSessionExpiryInterval:    PropTypeFourByteInt,
	PropAssignedClientIdentifier: PropTypeString,
	PropServerKeepAlive:          PropTypeTwoByteInt,
	PropAuthenticationMethod:     PropTypeString,
	PropAuthenticationData:       PropTypeBinary,
	PropRequestProblemInfo:       PropTypeByte,
	PropWillDelayInterval:        PropTypeFourByteInt,
	PropRequestResponseInfo:      PropTypeByte,
	PropResponseInformation:      PropTypeString,
	PropServerReference:          PropTypeString,
	PropReasonString:             PropTypeString,
	PropReceiveMaximum:           PropTypeTwoByteInt,
	PropTopicAliasMaximum:        PropTypeTwoByteInt,
	PropTopicAlias:               PropTypeTwoByteInt,
	PropMaximumQoS:               PropTypeByte,
	PropRetainAvailable:          PropTypeByte,
	PropUserProperty:             PropTypeStringPair,
	PropMaximumPacketSize:        PropTypeFourByteInt,
	PropWildcardSubAvailable:     PropTypeByte,
	PropSubscriptionIDAvailable:  PropTypeByte,
	PropSharedSubAvailable:       PropTypeByte,
}

// PropertyType returns the data type for this property ID.
func (p PropertyID) PropertyType() PropertyType {
	if t, ok := propertyTypeMap[p]; ok {
		return t
	}
	return PropTypeByte // default
}

// Property errors.
var (
	ErrUnknownPropertyID   = errors.New("unknown property identifier")
	ErrInvalidPropertyType = errors.New("invalid property type for identifier")
	ErrDuplicateProperty   = errors.New("duplicate property not allowed")
)

// Properties represents a collection of MQTT v5.0 properties.
type Properties struct {
	props []property
}

type property struct {
	id    PropertyID
	value any
}

// Len returns the number of properties in the collection.
func (p *Properties) Len() int {
	if p == nil {
		return 0
	}
	return len(p.props)
}

// Has returns true if the property with the given ID exists.
func (p *Properties) Has(id PropertyID) bool {
	if p == nil {
		return false
	}
	for i := range p.props {
		if p.props[i].id == id {
			return true
		}
	}
	return false
}

// Get returns the value of the property with the given ID.
// Returns nil if the property does not exist.
func (p *Properties) Get(id PropertyID) any {
	if p == nil {
		return nil
	}
	for i := range p.props {
		if p.props[i].id == id {
			return p.props[i].value
		}
	}
	return nil
}

// GetAll returns all values for properties with the given ID.
// Useful for properties that can appear multiple times (e.g., UserProperty, SubscriptionIdentifier).
func (p *Properties) GetAll(id PropertyID) []any {
	if p == nil {
		return nil
	}
	var result []any
	for i := range p.props {
		if p.props[i].id == id {
			result = append(result, p.props[i].value)
		}
	}
	return result
}

// Set sets a property value. For properties that can only appear once,
// this replaces any existing value.
func (p *Properties) Set(id PropertyID, value any) {
	if p == nil {
		return
	}
	// Check if property already exists and replace it
	for i := range p.props {
		if p.props[i].id == id {
			p.props[i].value = value
			return
		}
	}
	p.props = append(p.props, property{id: id, value: value})
}

// Add adds a property value. Use this for properties that can appear multiple times.
func (p *Properties) Add(id PropertyID, value any) {
	if p == nil {
		return
	}
	p.props = append(p.props, property{id: id, value: value})
}

// Delete removes all properties with the given ID.
func (p *Properties) Delete(id PropertyID) {
	if p == nil {
		return
	}
	n := 0
	for i := range p.props {
		if p.props[i].id != id {
			p.props[n] = p.props[i]
			n++
		}
	}
	p.props = p.props[:n]
}

// Typed getters

// GetByte returns the byte value of a property, or 0 if not found.
func (p *Properties) GetByte(id PropertyID) byte {
	v := p.Get(id)
	if v == nil {
		return 0
	}
	if b, ok := v.(byte); ok {
		return b
	}
	return 0
}

// GetUint16 returns the uint16 value of a property, or 0 if not found.
func (p *Properties) GetUint16(id PropertyID) uint16 {
	v := p.Get(id)
	if v == nil {
		return 0
	}
	if u, ok := v.(uint16); ok {
		return u
	}
	return 0
}

// GetUint32 returns the uint32 value of a property, or 0 if not found.
func (p *Properties) GetUint32(id PropertyID) uint32 {
	v := p.Get(id)
	if v == nil {
		return 0
	}
	if u, ok := v.(uint32); ok {
		return u
	}
	return 0
}

// GetString returns the string value of a property, or empty string if not found.
func (p *Properties) GetString(id PropertyID) string {
	v := p.Get(id)
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// GetBinary returns the binary value of a property, or nil if not found.
func (p *Properties) GetBinary(id PropertyID) []byte {
	v := p.Get(id)
	if v == nil {
		return nil
	}
	if b, ok := v.([]byte); ok {
		return b
	}
	return nil
}

// GetStringPair returns the string pair value of a property, or zero value if not found.
func (p *Properties) GetStringPair(id PropertyID) StringPair {
	v := p.Get(id)
	if v == nil {
		return StringPair{}
	}
	if sp, ok := v.(StringPair); ok {
		return sp
	}
	return StringPair{}
}

// GetAllStringPairs returns all string pair values for the given property ID.
func (p *Properties) GetAllStringPairs(id PropertyID) []StringPair {
	all := p.GetAll(id)
	if all == nil {
		return nil
	}
	result := make([]StringPair, 0, len(all))
	for _, v := range all {
		if sp, ok := v.(StringPair); ok {
			result = append(result, sp)
		}
	}
	return result
}

// GetAllVarInts returns all variable integer values for the given property ID.
func (p *Properties) GetAllVarInts(id PropertyID) []uint32 {
	all := p.GetAll(id)
	if all == nil {
		return nil
	}
	result := make([]uint32, 0, len(all))
	for _, v := range all {
		if u, ok := v.(uint32); ok {
			result = append(result, u)
		}
	}
	return result
}

// Encode writes the properties to the writer.
// Returns the number of bytes written.
func (p *Properties) Encode(w io.Writer) (int, error) {
	if p == nil || len(p.props) == 0 {
		return encodeVarint(w, 0)
	}

	// Calculate the size of the properties
	size := p.size()

	// Write the length as a variable byte integer
	n, err := encodeVarint(w, uint32(size))
	if err != nil {
		return n, err
	}

	// Write each property
	for i := range p.props {
		prop := &p.props[i]
		n2, err := p.encodeProperty(w, prop)
		n += n2
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

func (p *Properties) encodeProperty(w io.Writer, prop *property) (int, error) {
	// Write property ID
	n, err := w.Write([]byte{byte(prop.id)})
	if err != nil {
		return n, err
	}

	// Write value based on type
	propType := prop.id.PropertyType()
	var n2 int

	switch propType {
	case PropTypeByte:
		b, _ := prop.value.(byte)
		n2, err = w.Write([]byte{b})

	case PropTypeTwoByteInt:
		v, _ := prop.value.(uint16)
		n2, err = w.Write([]byte{byte(v >> 8), byte(v)})

	case PropTypeFourByteInt:
		v, _ := prop.value.(uint32)
		n2, err = w.Write([]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})

	case PropTypeVarInt:
		v, _ := prop.value.(uint32)
		n2, err = encodeVarint(w, v)

	case PropTypeString:
		s, _ := prop.value.(string)
		n2, err = encodeString(w, s)

	case PropTypeBinary:
		b, _ := prop.value.([]byte)
		n2, err = encodeBinary(w, b)

	case PropTypeStringPair:
		sp, _ := prop.value.(StringPair)
		n2, err = encodeStringPair(w, sp)
	}

	return n + n2, err
}

func (p *Properties) size() int {
	if p == nil {
		return 0
	}

	size := 0
	for i := range p.props {
		prop := &p.props[i]
		size++ // property ID

		propType := prop.id.PropertyType()
		switch propType {
		case PropTypeByte:
			size++
		case PropTypeTwoByteInt:
			size += 2
		case PropTypeFourByteInt:
			size += 4
		case PropTypeVarInt:
			v, _ := prop.value.(uint32)
			size += varintSize(v)
		case PropTypeString:
			s, _ := prop.value.(string)
			size += 2 + len(s)
		case PropTypeBinary:
			b, _ := prop.value.([]byte)
			size += 2 + len(b)
		case PropTypeStringPair:
			sp, _ := prop.value.(StringPair)
			size += 2 + len(sp.Key) + 2 + len(sp.Value)
		}
	}
	return size
}

// Decode reads properties from the reader.
// Returns the number of bytes read.
func (p *Properties) Decode(r io.Reader) (int, error) {
	// Read the length as a variable byte integer
	length, n, err := decodeVarint(r)
	if err != nil {
		return n, err
	}

	if length == 0 {
		return n, nil
	}

	// Read properties
	remaining := int(length)
	for remaining > 0 {
		// Read property ID
		var idBuf [1]byte
		n2, err := io.ReadFull(r, idBuf[:])
		n += n2
		remaining -= n2
		if err != nil {
			return n, err
		}

		id := PropertyID(idBuf[0])
		propType, ok := propertyTypeMap[id]
		if !ok {
			return n, ErrUnknownPropertyID
		}

		// Read value based on type
		var value any
		var n3 int

		switch propType {
		case PropTypeByte:
			var buf [1]byte
			n3, err = io.ReadFull(r, buf[:])
			value = buf[0]

		case PropTypeTwoByteInt:
			var buf [2]byte
			n3, err = io.ReadFull(r, buf[:])
			value = uint16(buf[0])<<8 | uint16(buf[1])

		case PropTypeFourByteInt:
			var buf [4]byte
			n3, err = io.ReadFull(r, buf[:])
			value = uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])

		case PropTypeVarInt:
			var v uint32
			v, n3, err = decodeVarint(r)
			value = v

		case PropTypeString:
			var s string
			s, n3, err = decodeString(r)
			value = s

		case PropTypeBinary:
			var b []byte
			b, n3, err = decodeBinary(r)
			value = b

		case PropTypeStringPair:
			var sp StringPair
			sp, n3, err = decodeStringPair(r)
			value = sp
		}

		n += n3
		remaining -= n3
		if err != nil {
			return n, err
		}

		p.props = append(p.props, property{id: id, value: value})
	}

	return n, nil
}
