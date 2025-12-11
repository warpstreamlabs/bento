package bundle

// AllConstructors is a set containing every single constructor that has been registered.
var AllConstructors = &ConstructorSet{
	ctors: []ManagedConstructor{},
}

//------------------------------------------------------------------------------

// ConstructorAdd adds a new constructor to this environment by providing a
// constructor function.
func (e *Environment) ConstructorAdd(ctor ManagedConstructor) error {
	// TODO(gregfurman): Should we be making these idempotent?
	return e.constructors.Add(ctor)
}

// ConstructorInit attempts to initilise all constructors registered against the environment.
func (e *Environment) ConstructorInit(mgr NewManagement) error {
	for _, ctor := range e.constructors.ctors {
		if err := ctor(mgr); err != nil {
			return wrapComponentErr(mgr, "constructor", err)
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// ManagedConstructor allows for a construction function to be registered against a manager.
type ManagedConstructor func(NewManagement) error

// ConstructorSet contains a set of managed constructor functions to be run against the environment.
type ConstructorSet struct {
	ctors []ManagedConstructor
}

// Add a new managed constructor function to this set.
func (s *ConstructorSet) Add(ctor ManagedConstructor) error {
	s.ctors = append(s.ctors, ctor)
	return nil
}
