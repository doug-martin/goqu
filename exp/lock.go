package exp

type (
	LockStrength int
	WaitOption   int
	Lock         interface {
		Strength() LockStrength
		WaitOption() WaitOption
	}
	lock struct {
		strength   LockStrength
		waitOption WaitOption
	}
)

const (
	ForNolock LockStrength = iota
	ForUpdate
	ForNoKeyUpdate
	ForShare
	ForKeyShare

	Wait WaitOption = iota
	NoWait
	SkipLocked
)

func NewLock(strength LockStrength, option WaitOption) Lock {
	return lock{
		strength:   strength,
		waitOption: option,
	}
}

func (l lock) Strength() LockStrength {
	return l.strength
}

func (l lock) WaitOption() WaitOption {
	return l.waitOption
}
