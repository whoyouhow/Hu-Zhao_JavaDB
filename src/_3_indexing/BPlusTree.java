package _3_indexing;


import java.util.ArrayList;

import _1_basic.Field;

public class BPlusTree {
    int pInner;
    int pLeaf;
    Node root;
//    LeafNode leafRoot;//used when the root is just a leaf
    boolean rootIsLeaf;
    
    public BPlusTree(int pInner, int pLeaf) {
     //your code here
     this.pInner = pInner;
     this.pLeaf = pLeaf;
//     this.root = new InnerNode(pInner, null);
     this.root = new LeafNode(pLeaf, null);
     this.rootIsLeaf = true;
    }
    
    public LeafNode search(Field f) {
     //your code here
     if (rootIsLeaf) {
      return ((LeafNode)root).contains(f) ? (LeafNode)root : null;
     }else {
      InnerNode currentInnerNode = (InnerNode)this.root;
      Node childNode = null;
      while(childNode == null || !childNode.isLeafNode()) {
       childNode = currentInnerNode.getChildByKey(f);
       if(childNode.isLeafNode()) {
        if(((LeafNode) childNode).contains(f)) {
         return (LeafNode) childNode;
        }
        return null;
       }
       else {
        currentInnerNode = (InnerNode) childNode; 
       }
      
      }
     
     return null;
     }
    }
    
    public void insert(Entry e) {
     //your code here
//     if(this.rootIsLeaf) {
//      Field pop = this.leafRoot.insert(e);
//      if(pop != null) {//the leafRoot is full
//       this.rootIsLeaf = false;
//      }
//     }
     if (rootIsLeaf) {
      Entry pop = ((LeafNode)root).insert(e);
      if(pop != null) {
       ArrayList<Node> splittedChild = ((LeafNode)root).split();
       root = new InnerNode(this.pInner, null);
       ((LeafNode)(splittedChild.get(0))).setParent((InnerNode)root);
       ((InnerNode)root).children.addAll(splittedChild);
       for(Node newChild: ((InnerNode)root).children) {
    	   if(newChild.isLeafNode()) {
    		   ((LeafNode)newChild).setParent((InnerNode)root);
    	   }
    	   else {
    		   ((InnerNode)newChild).setParent((InnerNode)root);
    	   }
       }
       ((InnerNode)root).entries.add(pop);
       this.rootIsLeaf = false;
       
      }
     }
     else {
      Entry pop = ((InnerNode)root).insert(e);
      if(pop != null) {
       ArrayList<Node> splittedChild = ((InnerNode)root).split();
       root = new InnerNode(this.pInner, null);
       ((InnerNode)(splittedChild.get(0))).setParent((InnerNode)root);
       ((InnerNode)root).children.addAll(splittedChild);
       for(Node newChild: ((InnerNode)root).children) {
    	   if(newChild.isLeafNode()) {
    		   ((LeafNode)newChild).setParent((InnerNode)root);
    	   }
    	   else {
    		   ((InnerNode)newChild).setParent((InnerNode)root);
    	   }
       }
       ((InnerNode)root).entries.add(pop);
       
      }
     }
    }
    
    public void delete(Entry e) {
     //your code here
    	if(this.rootIsLeaf) {
    		((LeafNode)this.root).delete(e);
    	}
    	else {
    		((InnerNode)this.root).delete(e);
    	}
    }
    
    public Node getRoot() {
     //your code here
     return this.root;
    }
    
    public void printTree() {
    	Node root = this.getRoot();
        ArrayList<Node> current = new ArrayList<Node>();
        ArrayList<Node> next = new ArrayList<Node>();
        current.add(root);
        while (true){
         if (!current.get(0).isLeafNode()) {
          for (int i = 0; i < current.size(); i ++) {
              System.out.print("{");
              if(((InnerNode)current.get(i)).getParent() !=null)
              System.out.print(((InnerNode)((InnerNode)current.get(i)).getParent()).getKeys());

           System.out.print(((InnerNode)current.get(i)).getKeys());
           for (int j = 0; j < ((InnerNode)current.get(i)).getChildren().size(); j ++) {
            next.add((Node) ((InnerNode)current.get(i)).getChildren().get(j));
           }
           System.out.print(" ");
           System.out.print("}");
          }
          System.out.println("");
          current = next;
          
          next = new ArrayList<Node>();
         }
         if (current.get(0).isLeafNode()) {
          for (int i = 0; i < current.size(); i ++) {
              System.out.print("{");
              if(((LeafNode)current.get(i)).getParent() !=null)
              System.out.print(((InnerNode)((LeafNode)current.get(i)).getParent()).getKeys());

              System.out.print(" ");
           System.out.print(((LeafNode)current.get(i)).getKeys());
//           System.out.print(" ");
//           System.out.print(((LeafNode)current.get(i)).entries.size());
           System.out.print("}");
          }
          System.out.println("");
          break;
         }
        }
        System.out.println("");
        
       }


 
}