package tag

import (
	"reflect"
	"strings"
)

// tagOptions is the string following a comma in a struct field's "json"
// tag, or the empty string. It does not include the leading comma.
type Tag struct {
	tag     string
	name    Name
	options Options
}
type Name string
type Options []string

func New(tagName string, st reflect.StructTag) Tag {
	return Parse(st.Get(tagName))
}
func (t Tag) Values() []string {
	tags := strings.Split(t.tag, ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	return tags
}
func Parse(tag string) Tag {
	var t Tag
	t.tag = tag
	tags := strings.Split(tag, ",")
	for i, v := range tags {
		tags[i] = strings.TrimSpace(v)
	}
	switch len(tags) {
	case 0:
		t.name = ""
		t.options = nil
	case 1:
		t.name = Name(tags[0])
		t.options = nil
	default:
		t.name = Name(tags[0])
		t.options = tags[1:]
	}
	return t
}
func (t Tag) Name() Name {
	return t.name
}

func (t Tag) IsEmpty() bool {
	return len(t.tag) == 0
}

func (t Tag) Has(optionName string) bool {
	return t.options.Contains(optionName)
}

func (n Name) IsEmpty() bool {
	if n != "" {
		return false
	}
	return true
}
func (n Name) String() string {
	return string(n)
}

// Contains reports whether a comma-separated list of options
// contains a particular substr flag. substr must be surrounded by a
// string boundary or commas.
func (o Options) Contains(optionName string) bool {
	if o.IsEmpty() {
		return false
	}
	for _, s := range o {
		if s == optionName {
			return true
		}
	}
	return false
}
func (o Options) IsEmpty() bool {
	return len(o) == 0
}
