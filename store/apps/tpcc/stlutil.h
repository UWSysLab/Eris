#ifndef STLUTIL_H__
#define STLUTIL_H__

// Deletes all elements in STL container.
template <typename T>
static void STLDeleteElements(T* container) {
    const typename T::iterator end = container->end();
    for (typename T::iterator i = container->begin(); i != end; ++i) {
        delete *i;
    }
    container->clear();
};

// Deletes all values (iterator->second) in STL container.
template <typename T>
static void STLDeleteValues(T* container) {
    const typename T::iterator end = container->end();
    for (typename T::iterator i = container->begin(); i != end; ++i) {
        delete i->second;
    }
    container->clear();
};

#endif
