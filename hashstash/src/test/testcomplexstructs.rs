use std::collections::HashMap;

use rand::prelude::*;

use crate::{
    test::testbasicstructs::StructA, test_stash_roundtrip, test_stash_roundtrip_inplace,
    InplaceUnstasher, Order, Stashable, Stasher, UnstashError, Unstashable, UnstashableInplace,
    Unstasher,
};

struct WeirdContainer<T> {
    items: Vec<Option<Box<T>>>,
}

impl<T> WeirdContainer<T> {
    fn new(capacity: usize) -> WeirdContainer<T> {
        let mut items = Vec::new();
        items.resize_with(capacity, || None);
        WeirdContainer { items }
    }

    fn items<'a>(&'a self) -> impl Iterator<Item = &'a T> {
        self.items.iter().filter_map(|i| match i {
            Some(i) => Some(&**i),
            None => None,
        })
    }

    fn clear(&mut self) {
        for item in &mut self.items {
            *item = None;
        }
    }

    fn insert_somewhere_random(&mut self, item: T) {
        let item = Box::new(item);
        let n = self.items.len();
        let idx = thread_rng().gen_range(0..n);
        for probe_idx in 0..n {
            let slot = &mut self.items[(idx + probe_idx) % n];
            if slot.is_none() {
                *slot = Some(item);
                return;
            }
        }
        panic!("WeirdContainer overflow");
    }

    fn scramble(&mut self) {
        self.items.shuffle(&mut thread_rng());
    }

    fn foreach_mut<F: FnMut(&mut T)>(&mut self, mut f: F) {
        for item in &mut self.items {
            if let Some(item) = item.as_mut() {
                f(item);
            }
        }
    }
}

struct StructWithWeirdContainer {
    container: WeirdContainer<StructA>,
}

impl Stashable for StructWithWeirdContainer {
    fn stash(&self, stasher: &mut Stasher) {
        stasher.array_of_objects_iter(self.container.items(), Order::Unordered);
    }
}

impl Unstashable for StructWithWeirdContainer {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        let mut container = WeirdContainer::<StructA>::new(1024);
        for s in unstasher.array_of_objects_iter::<StructA>()? {
            container.insert_somewhere_random(s?);
        }
        Ok(StructWithWeirdContainer { container })
    }
}

impl UnstashableInplace for StructWithWeirdContainer {
    fn unstash_inplace(
        &mut self,
        unstasher: &mut InplaceUnstasher<()>,
    ) -> Result<(), UnstashError> {
        let mut temp_objects: Vec<StructA> = Vec::new();
        unstasher.array_of_objects_vec_inplace(&mut temp_objects)?;
        if unstasher.time_to_write() {
            self.container.clear();
            for object in temp_objects {
                self.container.insert_somewhere_random(object);
            }
        }
        Ok(())
    }
}

#[test]
fn test_roundtrip_weird_container() {
    let create = || {
        let mut container = WeirdContainer::<StructA>::new(1024);
        container.insert_somewhere_random(StructA {
            i: 1,
            x: 2,
            s: "three".to_string(),
        });
        container.insert_somewhere_random(StructA {
            i: 9,
            x: 21,
            s: "threee".to_string(),
        });
        container.insert_somewhere_random(StructA {
            i: 909,
            x: 42,
            s: "threeeeeeeee".to_string(),
        });
        StructWithWeirdContainer { container }
    };

    let modify_1 = |s: &mut StructWithWeirdContainer| {
        s.container.insert_somewhere_random(StructA {
            i: 4,
            x: 44,
            s: "four hundred and forty four".to_string(),
        });
    };
    let modify_2 = |s: &mut StructWithWeirdContainer| {
        s.container.scramble();
        s.container.insert_somewhere_random(StructA {
            i: 90,
            x: 91,
            s: "dcba".to_string(),
        });
    };
    let modify_3 = |s: &mut StructWithWeirdContainer| {
        s.container.foreach_mut(|a| a.x *= 2);
    };

    assert_eq!(test_stash_roundtrip(create, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create, modify_3, (), ()), Ok(()));
    assert_eq!(
        test_stash_roundtrip_inplace(create, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create, modify_3, (), ()),
        Ok(())
    );
}

struct GraphNode {
    id: i32,
    data: Vec<u8>,
    inputs: Vec<i32>,
}

impl GraphNode {
    fn new(id: i32, data: Vec<u8>) -> GraphNode {
        GraphNode {
            id,
            data,
            inputs: Vec::new(),
        }
    }

    fn set_data(&mut self, data: Vec<u8>) {
        self.data = data;
    }
}

struct Graph {
    nodes: HashMap<i32, GraphNode>,
}

impl Graph {
    fn new() -> Graph {
        Graph {
            nodes: HashMap::new(),
        }
    }

    fn add_node(&mut self, id: i32, data: Vec<u8>) {
        self.nodes.insert(id, GraphNode::new(id, data));
    }

    fn remove_node(&mut self, id: i32) {
        self.nodes.remove(&id).unwrap();
    }

    fn node_mut(&mut self, id: i32) -> Option<&mut GraphNode> {
        self.nodes.get_mut(&id)
    }

    fn connect_nodes(&mut self, src: i32, dst: i32) {
        self.nodes.get_mut(&src).unwrap().inputs.push(dst);
    }

    fn disconnect_node(&mut self, id: i32) {
        self.nodes.get_mut(&id).unwrap().inputs.clear();
    }

    fn node_ids(&self) -> Vec<i32> {
        self.nodes.keys().cloned().collect()
    }
}

impl Stashable for Graph {
    fn stash(&self, stasher: &mut Stasher) {
        // nodes
        stasher.array_of_proxy_objects(
            self.nodes.values(),
            |node, stasher| {
                stasher.i32(node.id);
                stasher.array_of_u8_slice(&node.data);
            },
            Order::Unordered,
        );

        // connections
        // (these could also be serialized with each node,
        // but it proves a more interesting point to do it
        // separately, which is support for different APIs
        // that require separate explicit actions for stuff)
        let connect_src_dst_pairs = self
            .nodes
            .values()
            .map(|node| {
                node.inputs
                    .iter()
                    .map(|dst| -> [i32; 2] { [node.id, *dst] })
            })
            .flatten();

        stasher.array_of_proxy_objects(
            connect_src_dst_pairs,
            |[src, dst], stasher| {
                stasher.i32(*src);
                stasher.i32(*dst);
            },
            Order::Unordered,
        );
    }
}

impl Unstashable for Graph {
    fn unstash(unstasher: &mut Unstasher) -> Result<Self, UnstashError> {
        let mut graph = Graph::new();

        unstasher.array_of_proxy_objects(|unstasher| {
            let id = unstasher.i32()?;
            let data = unstasher.array_of_u8_vec()?;
            graph.add_node(id, data);
            Ok(())
        })?;

        unstasher.array_of_proxy_objects(|unstasher| {
            let src = unstasher.i32()?;
            let dst = unstasher.i32()?;
            graph.connect_nodes(src, dst);
            Ok(())
        })?;

        Ok(graph)
    }
}

impl UnstashableInplace for Graph {
    fn unstash_inplace(
        &mut self,
        unstasher: &mut InplaceUnstasher<()>,
    ) -> Result<(), UnstashError> {
        let time_to_write = unstasher.time_to_write();

        let mut node_ids_to_keep = Vec::<i32>::new();

        unstasher.array_of_proxy_objects(|u| {
            let id = u.i32()?;
            let data = u.array_of_u8_vec()?;

            if time_to_write {
                if let Some(node) = self.node_mut(id) {
                    // Preserve existing nodes with matching ids
                    node.set_data(data);
                } else {
                    // Add new nodes as needed
                    self.add_node(id, data);
                }

                node_ids_to_keep.push(id);
            }

            Ok(())
        })?;

        // Remove unreferenced nodes
        if time_to_write {
            for id in self.node_ids() {
                if !node_ids_to_keep.contains(&id) {
                    self.remove_node(id);
                }
            }
        }

        // Clear all connections
        if time_to_write {
            for id in self.node_ids() {
                self.disconnect_node(id);
            }
        }

        // Add back unstashed connections
        unstasher.array_of_proxy_objects(|u| {
            let src = u.i32()?;
            let dst = u.i32()?;

            if time_to_write {
                self.connect_nodes(src, dst);
            }

            Ok(())
        })?;

        Ok(())
    }
}

#[test]
fn test_graph_roundtrip() {
    let create_1 = || Graph::new();
    let create_2 = || {
        let mut graph = Graph::new();
        graph.add_node(1, vec![]);
        graph.add_node(2, vec![0x0]);
        graph
    };
    let create_3 = || {
        let mut graph = Graph::new();
        graph.add_node(1, vec![]);
        graph.add_node(2, vec![0x0]);
        graph.connect_nodes(1, 2);
        graph.connect_nodes(2, 2);
        graph
    };

    let modify_1 = |graph: &mut Graph| {
        graph.add_node(100, vec![0xd, 0xe, 0xf]);
    };

    let modify_2 = |graph: &mut Graph| {
        graph.add_node(50, vec![0xf]);
        for i in 51..=60 {
            graph.add_node(i, vec![i as u8]);
            graph.connect_nodes(i, 50);
        }
    };

    assert_eq!(test_stash_roundtrip(create_1, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_1, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_1, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_2, modify_2, (), ()), Ok(()));
    assert_eq!(test_stash_roundtrip(create_3, modify_2, (), ()), Ok(()));

    assert_eq!(
        test_stash_roundtrip_inplace(create_1, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_2, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_3, modify_1, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_1, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_2, modify_2, (), ()),
        Ok(())
    );
    assert_eq!(
        test_stash_roundtrip_inplace(create_3, modify_2, (), ()),
        Ok(())
    );
}
