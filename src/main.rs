use std::error::Error;
use std::io::BufReader;
use std::fs::File;
// use serde::Deserialize;

extern crate serde;
#[macro_use]
extern crate serde_derive;

#[derive(Debug, Deserialize)]
struct Iris{
    sepal_length: f64,
    sepal_width: f64,
    petal_length: f64,
    petal_width: f64,
    variety: String,

} 

#[derive(Debug, Clone)]
struct Node {
    vs: Vec<i64>,
    tag: Option<i64>
}
impl Node {
    fn distance_square_from(&self, node: &Node) -> i64 {
        let min_len = std::cmp::min(self.vs.len(), node.vs.len());
        let mut sum = 0;
        for i in 0..min_len {
            sum += (self.vs[i] - node.vs[i]) * (self.vs[i] - node.vs[i]);
        }
        sum
    }
}

#[derive(Debug, Clone)]
struct KdNode {
    node: Node,
    left: Option<Box<KdNode>>,
    right: Option<Box<KdNode>>,
}

impl KdNode {
    fn new(node: Node) -> Self {
        Self{
            node: node,
            left: None,
            right: None 
        }
    }
}

fn read_iris(filename: &str) -> Result<Vec<Iris>, Box<dyn Error>> {
    let f = File::open(filename)?;
    let f = BufReader::new(f);
    let mut rdr = csv::Reader::from_reader(f);
    let mut vec = Vec::new();
    for result in rdr.deserialize() {
        let record: Iris = result?;
        vec.push(record);
    }
    Ok(vec)
}

fn mid3(x:i64, y:i64, z:i64) -> i64 {
    if x < y {
        if y < z {
            y 
        } else if z < x {
            x
        } else {
            z
        }
    } else {
        if z < y {
            y 
        }else if x < z {
            x
        } else {
            z
        }
    }
}

fn like_qsort(nodes: &[Node], indices: &mut [usize], left: usize, right: usize, depth: usize) -> Option<KdNode> {
    if left > right {
        return None;
    } else if left == right {
        return Some(KdNode::new(nodes[indices[left]].clone()));
    }
    let axis = depth % 4;

    let pivot = mid3(nodes[indices[left]].vs[axis], nodes[indices[left + ((right-left)>>1)]].vs[axis], nodes[indices[right]].vs[axis]);//indices[left + ((right-left)>>1)];


    let mut l = left;
    let mut r = right;
    while l <= r {
        while r > left  && pivot < nodes[indices[r]].vs[axis] {
            r = r - 1;
        }
        while l < right && nodes[indices[l]].vs[axis] < pivot {
            l = l + 1;
        }
        if l <= r   {
            indices.swap(l, r);
            l = l + 1;
            if r > 0 {
                r = r - 1;
            }
        }
    }

    let mid = 
        if l - r == 2 {
            l - 1 // or r + 1
        } else if l - r == 1 {
            if  r - left > right - l {
                let mut i = r;
                let mut max_v = nodes[indices[i]].vs[axis];
                for x in left..(r+1) {
                    if  max_v < nodes[indices[x]].vs[axis] {
                        i = x;
                        max_v = nodes[indices[x]].vs[axis];
                    }
                }
                indices.swap(r, i);
                r
            }  else {
                let mut i = l;
                let mut min_v = nodes[indices[i]].vs[axis];
                for x in l..(right+1) {
                    if  min_v > nodes[indices[x]].vs[axis] {
                        i = x;
                        min_v = nodes[indices[x]].vs[axis];
                    }
                }
                indices.swap(l, i);
                l
            }     
        } else {
            assert!(false);
            0 as usize
        };
    
    // for x in (left..mid) {
    //     assert!(nodes[indices[mid]].vs[axis] >= nodes[indices[x]].vs[axis])
    // }
    // for x in mid+1..(right+1) {
    //     assert!(nodes[indices[mid]].vs[axis] <= nodes[indices[x]].vs[axis])
    // }
    let left_kd_node = if mid > 0{
            like_qsort(nodes, indices, left, mid - 1, depth + 1).map(|x| Box::new(x))    
        } else {
            None
        };
    let right_kd_node = like_qsort(nodes, indices, mid + 1, right, depth + 1).map(|x| Box::new(x));

    Some(KdNode {
        node: nodes[indices[mid]].clone(),
        left: left_kd_node,
        right: right_kd_node
    })
}


fn search<'a>(cur: &'a KdNode, node: &Node, min_distance: i64, min_kd_node: &'a KdNode, depth: usize) -> (i64, &'a KdNode) {
    let axis = depth % 4;
    let given_axis_value = node.vs[axis].clone();
    
    let min_distance = std::cmp::min(min_distance, node.distance_square_from(&cur.node));
    let ((min_distance, min_kd_node), next) = if cur.node.vs[axis] > given_axis_value {
        (cur.left.as_ref()
            .map(|kdn| search(&kdn, node, min_distance, &kdn, depth+1) )
            .unwrap_or((min_distance, min_kd_node)), cur.right.as_ref())
    } else {
        (cur.right.as_ref()
            .map(|kdn| search(&kdn, node, min_distance, &kdn, depth+1) )
            .unwrap_or((min_distance, min_kd_node)), cur.left.as_ref())
    };

    let should_search_next = match next {
        Some(x) => {let d = x.node.vs[axis] - given_axis_value;d*d <= min_distance},
        None => false
    };

    if should_search_next {
        println!("next -> {}", min_distance);
        search(next.unwrap(), node, min_distance, min_kd_node, depth+1)
    } else {
        (min_distance, min_kd_node)
    }

}

fn kdtree(nodes: Vec<Node>) {
    let mut indices: Vec<usize>= (0..nodes.len()).collect();
    let kd_tree = like_qsort(&nodes, &mut indices, 0, nodes.len()-1, 0).unwrap();
    
    // 5.9,3,5.1,1.8,"Virginica"
    let node = Node{
        vs: vec![(5.9*1000.0) as i64, (3.0*1000.0) as i64, (5.1*1000.0) as i64, (1.8*1000.0) as i64],
        tag: Some(3)
    };
    let (min_distance, min_kd) = search(&kd_tree, &node, 1000000000, &kd_tree, 0);
    println!("{} {} ({}, {}, {}, {})",min_distance, min_kd.node.tag.unwrap_or(-1), min_kd.node.vs[0], min_kd.node.vs[1], min_kd.node.vs[2], min_kd.node.vs[3]);
} 

fn main() {

    let records = read_iris("data/iris.csv").unwrap();
    let nodes: Vec<Node> = records.iter()
        .map(|s| {
                let tagNum: Option <i64> = match &*(s.variety) {
                    "variety"    => Some(0),
                    "Setosa"     => Some(1),
                    "Versicolor" => Some(2),
                    "Virginica"  => Some(3),
                    _            => None
                };
                Node{
                    vs: vec![(s.sepal_length*1000.0) as i64, (s.sepal_width*1000.0) as i64, (s.petal_length*1000.0) as i64, (s.petal_width*1000.0) as i64],
                    tag: tagNum
                }
            }
        ).collect();
    println!("success {}", records.len());
    kdtree(nodes);
}
