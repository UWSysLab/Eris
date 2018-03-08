#ifndef BTREE_H__
#define BTREE_H__

#include <string.h>

#include <boost/static_assert.hpp>
#include <boost/pool/object_pool.hpp>
#include "store/apps/tpcc/assert.h"

// DEBUG
#include <iostream>
using std::cout;
using std::endl;

#ifdef __linux__
#define HAVE_POSIX_MEMALIGN
#endif

#ifdef HAVE_POSIX_MEMALIGN
// Nothing to do
#else
// TODO: This is not aligned! It doesn't matter for this implementation, but it could
static inline int posix_memalign(void** result, int, size_t bytes) {
    *result = malloc(bytes);
    return *result == NULL;
}
#endif

template <typename KEY, typename VALUE, unsigned N, unsigned M,
        unsigned INNER_NODE_PADDING= 0, unsigned LEAF_NODE_PADDING= 0,
        unsigned NODE_ALIGNMENT= 64>
class BPlusTree
{
public:
        // N must be greater than two to make the split of
        // two inner nodes sensible.
        BOOST_STATIC_ASSERT(N>2);
        // Leaf nodes must be able to hold at least one element
        BOOST_STATIC_ASSERT(M>0);

        // Builds a new empty tree.
        BPlusTree()
        : depth(0),
          root(new_leaf_node())
        {
                // DEBUG
                // cout << "sizeof(LeafNode)==" << sizeof(LeafNode) << endl;
                // cout << "sizeof(InnerNode)==" << sizeof(InnerNode) << endl;
        }

        ~BPlusTree() {
                // Empty. Memory deallocation is done automatically
                // when innerPool and leafPool are destroyed.
        }

        // Inserts a pair (key, value). If there is a previous pair with
        // the same key, the old value is overwritten with the new one.
        void insert(KEY key, VALUE value) {
                // GCC warns that this may be used uninitialized, even though that is untrue.
                InsertionResult result = { KEY(), 0, 0 };
                bool was_split;
                if( depth == 0 ) {
                        // The root is a leaf node
                        assert( *reinterpret_cast<NodeType*>(root) == 
                                NODE_LEAF);
                        was_split= leaf_insert(reinterpret_cast<LeafNode*>
                                (root), key, value, &result);
                } else {
                        // The root is an inner node
                        assert( *reinterpret_cast<NodeType*>
                                (root) == NODE_INNER );
                        was_split= inner_insert(reinterpret_cast<InnerNode*>
                                (root), depth, key, value, &result);
                }
                if( was_split ) {
                        // The old root was splitted in two parts.
                        // We have to create a new root pointing to them
                        depth++;
                        root= new_inner_node();
                        InnerNode* rootProxy=
                                reinterpret_cast<InnerNode*>(root);
                        rootProxy->num_keys= 1;
                        rootProxy->keys[0]= result.key;
                        rootProxy->children[0]= result.left;
                        rootProxy->children[1]= result.right;
                }
        }

// Looks for the given key. If it is not found, it returns false,
// if it is found, it returns true and copies the associated value
// unless the pointer is null.
bool find(const KEY& key, VALUE* value= 0) const {
  const InnerNode* inner;
  register const void* node= root;
  register unsigned d= depth, index;
  while( d-- != 0 ) {
    inner= reinterpret_cast<const InnerNode*>(node);
    assert( inner->type == NODE_INNER );
    index= inner_position_for(key, inner->keys, inner->num_keys);
    assert(index < inner->num_keys + 1);
    node= inner->children[index];
  }
  const LeafNode* leaf= reinterpret_cast<const LeafNode*>(node);
  assert( leaf->type == NODE_LEAF );
  index= leaf_position_for(key, leaf->keys, leaf->num_keys);
  assert(index <= leaf->num_keys);
  if( index < leaf->num_keys && leaf->keys[index] == key ) {
    if( value != 0 ) {
      *value= leaf->values[index];
    }
    if (leaf->values[index])
      return true;
    else return false;
  } else {
    return false;
  }
}


// Looks for the given key. If it is not found, it returns false,
// if it is found, it returns true and sets 
// the associated value to NULL
// Note: del currently leaks memory. Fix later.
bool del(const KEY& key) {
  InnerNode* inner;
  register void* node= root;
  register unsigned d= depth, index;
  while( d-- != 0 ) {
    inner= reinterpret_cast<InnerNode*>(node);
    assert( inner->type == NODE_INNER );
    index= inner_position_for(key, inner->keys, inner->num_keys);
    node= inner->children[index];
  }
  LeafNode* leaf= reinterpret_cast<LeafNode*>(node);
  assert( leaf->type == NODE_LEAF );
  index= leaf_position_for(key, leaf->keys, leaf->num_keys);
  if( leaf->keys[index] == key ) {
    leaf->values[index] = 0;
    return true;
  } else {
    return false;
  }
}

// Finds the LAST item that is < key. That is, the next item in the tree is not < key, but this
// item is. If we were to insert key into the tree, it would go after this item. This is weird,
// but is easier than implementing iterators. In STL terms, this would be "lower_bound(key)--"
// WARNING: This does *not* work when values are deleted. Thankfully, TPC-C does not use deletes.
bool findLastLessThan(const KEY& key, VALUE* value = 0, KEY* out_key = 0) const {
    const void* node = root;
    unsigned int d = depth;
    while( d-- != 0 ) {
        const InnerNode* inner = reinterpret_cast<const InnerNode*>(node);
        assert( inner->type == NODE_INNER );
        unsigned int pos = inner_position_for(key, inner->keys, inner->num_keys);
        // We need to rewind in the case where they are equal
        if (pos > 0 && key == inner->keys[pos-1]) {
            pos -= 1;
        }
        assert(pos == 0 || inner->keys[pos-1] < key);
        node = inner->children[pos];
    }
    const LeafNode* leaf= reinterpret_cast<const LeafNode*>(node);
    assert( leaf->type == NODE_LEAF );
    unsigned int pos = leaf_position_for(key, leaf->keys, leaf->num_keys);
    if (pos <= leaf->num_keys) {
        pos -= 1;
        if (pos < leaf->num_keys && key == leaf->keys[pos]) {
            pos -= 1;
        }

        if (pos < leaf->num_keys) {
            assert(leaf->keys[pos] < key);
            if (leaf->values[pos]) {
                if (value != NULL) {
                    *value = leaf->values[pos];
                }
                if (out_key != NULL) {
                    *out_key = leaf->keys[pos];
                }
                return true;
            } else {
                // This is a deleted key! Try again with the new key value
                // HACK: This is because this implementation doesn't do deletes correctly. However,
                // this makes it work. We need this for TPC-C undo. The solution is to use a
                // more complete b-tree implementation.
                return findLastLessThan(leaf->keys[pos], value, out_key);
            }
        }
    }

    return false;
}

        // Returns the size of an inner node
        // It is useful when optimizing performance with cache alignment.
        unsigned sizeof_inner_node() const {
                return sizeof(InnerNode);
        }

        // Returns the size of a leaf node.
        // It is useful when optimizing performance with cache alignment.
        unsigned sizeof_leaf_node() const {
                return sizeof(LeafNode);
        }
        

private:
        // Used when debugging
        enum NodeType {NODE_INNER=0xDEADBEEF, NODE_LEAF=0xC0FFEE};

        // Leaf nodes store pairs of keys and values.
        struct LeafNode {
#ifndef NDEBUG
	  // Leaf nodes need to memset the keys: when inserting the first key, we
      // check to see if we need to overwrite or insert
	  LeafNode() : type(NODE_LEAF), num_keys(0) {memset(keys,0,sizeof(keys));}
                const NodeType type;
#else
                LeafNode() : num_keys(0) {memset(keys,0,sizeof(keys));}
#endif
                unsigned num_keys;
                KEY      keys[M];
                VALUE    values[M];
	  //                unsigned char _pad[LEAF_NODE_PADDING];
        };

        // Inner nodes store pointers to other nodes interleaved with keys.
        struct InnerNode {
#ifndef NDEBUG
                InnerNode() : type(NODE_INNER), num_keys(0) {}
                const NodeType type;
#else
                InnerNode() : num_keys(0) {}
#endif
                unsigned num_keys;
                KEY      keys[N];
                void*    children[N+1];
		//                unsigned char _pad[INNER_NODE_PADDING];
        };

        // Custom allocator that returns aligned blocks of memory
        template <unsigned ALIGNMENT>
        struct AlignedMemoryAllocator {
                typedef std::size_t size_type;
                typedef std::ptrdiff_t difference_type;
                
                static char* malloc(const size_type bytes)
                {
                        void* result;
                        if( posix_memalign(&result, ALIGNMENT, bytes) != 0 ) {
                                result= 0;
                        }
                        // Alternative: result= std::malloc(bytes);
                        return reinterpret_cast<char*>(result);
                }
                static void free(char* const block)
                { std::free(block); }
        };

        // Returns a pointer to a fresh leaf node.
        LeafNode* new_leaf_node() {
                LeafNode* result;
                //result= new LeafNode();
                result= leafPool.construct();
                //cout << "New LeafNode at " << result << endl;
                return result;
        }

        // Frees a leaf node previously allocated with new_leaf_node()
        void delete_leaf_node(LeafNode* node) {
                assert( node->type == NODE_LEAF );
                //cout << "Deleting LeafNode at " << node << endl;
                // Alternatively: delete node;
                leafPool.destroy(node);
        }

        // Returns a pointer to a fresh inner node.
        InnerNode* new_inner_node() {
                InnerNode* result;
                // Alternatively: result= new InnerNode();
                result= innerPool.construct();
                //cout << "New InnerNode at " << result << endl;
                return result;
        }

        // Frees an inner node previously allocated with new_inner_node()
        void delete_inner_node(InnerNode* node) {
                assert( node->type == NODE_INNER );
                //cout << "Deleting InnerNode at " << node << endl;
                // Alternatively: delete node;
                innerPool.destroy(node);
        }
        
        // Data type returned by the private insertion methods.
        struct InsertionResult {
                KEY key;
                void* left;
                void* right;
        };

        // Returns the position where 'key' should be inserted in a leaf node
        // that has the given keys.
        static unsigned leaf_position_for(const KEY& key, const KEY* keys,
                        unsigned num_keys) {
                // Simple linear search. Faster for small values of N or M
                unsigned k= 0;
                while((k < num_keys) && (keys[k]<key)) {
                        ++k;
                }
                return k;
                /*
                // Binary search. It is faster when N or M is > 100,
                // but the cost of the division renders it useless
                // for smaller values of N or M.
                XXX--- needs to be re-checked because the linear search
                has changed since the last update to the following ---XXX
                int left= -1, right= num_keys, middle;
                while( right -left > 1 ) {
                        middle= (left+right)/2;
                        if( keys[middle] < key) {
                                left= middle;
                        } else {
                                right= middle;
                        }
                }
                //assert( right == k );
                return unsigned(right);
                */
        }

        // Returns the position where 'key' should be inserted in an inner node
        // that has the given keys.
        static inline unsigned inner_position_for(const KEY& key, const KEY* keys,
                        unsigned num_keys) {
                // Simple linear search. Faster for small values of N or M
                unsigned k= 0;
                while((k < num_keys) && ((keys[k]<key) || (keys[k]==key))) {
                        ++k;
                }
                return k;
                // Binary search is faster when N or M is > 100,
                // but the cost of the division renders it useless
                // for smaller values of N or M.
        }

        bool leaf_insert(LeafNode* node, KEY& key,
                        VALUE& value, InsertionResult* result) {
                assert( node->type == NODE_LEAF );
                assert( node->num_keys <= M );
                bool was_split= false;
                // Simple linear search
                unsigned i= leaf_position_for(key, node->keys, node->num_keys);
                if( node->num_keys == M ) {
                        // The node was full. We must split it
                        unsigned treshold= (M+1)/2;
                        LeafNode* new_sibling= new_leaf_node();
                        new_sibling->num_keys= node->num_keys -treshold;
                        for(unsigned j=0; j < new_sibling->num_keys; ++j) {
                                new_sibling->keys[j]= node->keys[treshold+j];
                                new_sibling->values[j]=
                                        node->values[treshold+j];
                        }
                        node->num_keys= treshold;
                        if( i < treshold ) {
                                // Inserted element goes to left sibling
                                leaf_insert_nonfull(node, key, value, i);
                        } else {
                                // Inserted element goes to right sibling
                                leaf_insert_nonfull(new_sibling, key, value,
                                        i-treshold);
                        }
                        // Notify the parent about the split
                        was_split= true;
                        result->key= new_sibling->keys[0];
                        result->left= node;
                        result->right= new_sibling;
                } else {
                        // The node was not full
                        leaf_insert_nonfull(node, key, value, i);
                }
                return was_split;
        }

        static void leaf_insert_nonfull(LeafNode* node, KEY& key, VALUE& value,
                        unsigned index) {
                assert( node->type == NODE_LEAF );
                assert( node->num_keys < M );
                assert( index < M );
                assert( index <= node->num_keys );
                if( node->keys[index] == key ) {
                        // We are inserting a duplicate value.
                        // Simply overwrite the old one
                        node->values[index]= value;
                } else {
                        // The key we are inserting is unique
                        for(unsigned i=node->num_keys; i > index; --i) {
                                node->keys[i]= node->keys[i-1];
                                node->values[i]= node->values[i-1];
                        }
                        node->num_keys++;
                        node->keys[index]= key;
                        node->values[index]= value;
                }
        }

        bool inner_insert(InnerNode* node, unsigned current_depth, KEY& key,
                        VALUE& value, InsertionResult* result) {
                assert( node->type == NODE_INNER );
                assert( current_depth != 0 );
                // Early split if node is full.
                // This is not the canonical algorithm for B+ trees,
                // but it is simpler and does not break the definition.
                bool was_split= false;
                if( node->num_keys == N ) {
                        // Split
                        unsigned treshold= (N+1)/2;
                        InnerNode* new_sibling= new_inner_node();
                        new_sibling->num_keys= node->num_keys -treshold;
                        for(unsigned i=0; i < new_sibling->num_keys; ++i) {
                                new_sibling->keys[i]= node->keys[treshold+i];
                                new_sibling->children[i]=
                                        node->children[treshold+i];
                        }
                        new_sibling->children[new_sibling->num_keys]=
                                node->children[node->num_keys];
                        node->num_keys= treshold-1;
                        // Set up the return variable
                        was_split= true;
                        result->key= node->keys[treshold-1];
                        result->left= node;
                        result->right= new_sibling;
                        // Now insert in the appropriate sibling
                        if( key < result->key ) {
                                inner_insert_nonfull(node, current_depth, key, value);
                        } else {
                                inner_insert_nonfull(new_sibling, current_depth, key,
                                        value);
                        }
                } else {
                        // No split
                        inner_insert_nonfull(node, current_depth, key, value);
                }
                return was_split;
        }

        void inner_insert_nonfull(InnerNode* node, unsigned current_depth, KEY& key,
                        VALUE& value) {
                assert( node->type == NODE_INNER );
                assert( node->num_keys < N );
                assert( current_depth != 0 );
                // Simple linear search
                unsigned index= inner_position_for(key, node->keys,
                        node->num_keys);
                // GCC warns that this may be used uninitialized, even though that is untrue.
                InsertionResult result = { KEY(), 0, 0 };
                bool was_split;
                if( current_depth-1 == 0 ) {
                        // The children are leaf nodes
                        for(unsigned kk=0; kk < node->num_keys+1; ++kk) {
                                assert( *reinterpret_cast<NodeType*>
                                        (node->children[kk]) == NODE_LEAF );
                        }
                        was_split= leaf_insert(reinterpret_cast<LeafNode*>
                                (node->children[index]), key, value, &result);
                } else {
                        // The children are inner nodes
                        for(unsigned kk=0; kk < node->num_keys+1; ++kk) {
                                assert( *reinterpret_cast<NodeType*>
                                        (node->children[kk]) == NODE_INNER );
                        }
                        InnerNode* child= reinterpret_cast<InnerNode*>
                                (node->children[index]);
                        was_split= inner_insert( child, current_depth-1, key, value,
                                &result);
                }
                if( was_split ) {
                        if( index == node->num_keys ) {
                                // Insertion at the rightmost key
                                node->keys[index]= result.key;
                                node->children[index]= result.left;
                                node->children[index+1]= result.right;
                                node->num_keys++;
                        } else {
                                // Insertion not at the rightmost key
                                node->children[node->num_keys+1]=
                                        node->children[node->num_keys];
                                for(unsigned i=node->num_keys; i!=index; --i) {
                                        node->children[i]= node->children[i-1];
                                        node->keys[i]= node->keys[i-1];
                                }
                                node->children[index]= result.left;
                                node->children[index+1]= result.right;
                                node->keys[index]= result.key;
                                node->num_keys++;
                        }
                } // else the current node is not affected
        }

        typedef AlignedMemoryAllocator<NODE_ALIGNMENT> AlignedAllocator;

        // Node memory allocators. IMPORTANT NOTE: they must be declared
        // before the root to make sure that they are properly initialised
        // before being used to allocate any node.
        boost::object_pool<InnerNode, AlignedAllocator> innerPool;
        boost::object_pool<LeafNode, AlignedAllocator>  leafPool;
        // Depth of the tree. A tree of depth 0 only has a leaf node.
        unsigned depth;
        // Pointer to the root node. It may be a leaf or an inner node, but
        // it is never null.
        void*    root;
};

#endif // !defined BPLUSTREE_HPP_227824
