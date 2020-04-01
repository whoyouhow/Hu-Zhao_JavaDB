package _3_indexing;

import java.util.ArrayList;

import _1_basic.Field;
import _1_basic.RelationalOperator;

public class InnerNode implements Node {
	int degree;
	ArrayList<Entry> entries;
	ArrayList<Node> children;
	InnerNode parent;
	int lhe;
	int minEntries;
	
	public InnerNode(int degree, InnerNode parent) {
		//your code here
		this.degree = degree;
		this.entries = new ArrayList<Entry>();
		this.children = new ArrayList<Node>();
		this.parent = parent;
		this.lhe = (degree) / 2;
		this.minEntries = (degree + 1) / 2;
//		this.lhe = this.degree % 2 == 0 ? (degree-1)/2 : (degree/2);
		
		
//		this.lhe = Math.round((float)degree/2);
	}
	
	public ArrayList<Field> getKeys() {
		//your code here
		ArrayList<Field> keys = new ArrayList<Field>();
		for(Entry entry: this.entries) {
			keys.add(entry.getField());
		}
		return keys;
	}
	
	public ArrayList<Node> getChildren() {
		//your code here
		return this.children;
	}

	public int getDegree() {
		//your code here
		return this.degree;
	}
	
	public boolean isLeafNode() {
		return false;
	}
	
	public InnerNode getParent() {
		return this.parent;
	}
	
	public InnerNode getAncestorWithLeft() {
		InnerNode currentNode = this.parent;
		while(currentNode != null) {
			int index = currentNode.getChildIndexByKey(this.entries.get(0).getField());
			if(index != 0) {
				return currentNode;
			}
			currentNode = currentNode.parent;
		}
		return null;
	}
	
	public InnerNode getAncestorWithRight() {
		InnerNode currentNode = this.parent;
		while(currentNode != null) {
			int index = currentNode.getChildIndexByKey(this.entries.get(0).getField());
//			System.out.println(index);
			if(index < currentNode.children.size() - 1) {
				return currentNode;
			}
			currentNode = currentNode.parent;
		}
		return null;
	}
	
	public InnerNode getLeftSibling() {
		InnerNode currentNode = this.parent;
		int layersFromAncestor = 0;
		while(currentNode != null) {
			layersFromAncestor ++;
			int index = currentNode.getChildIndexByKey(this.entries.get(0).getField());
			if(index != 0) {
				while(layersFromAncestor > 0) {
					currentNode = (InnerNode)currentNode.children.get(index - 1);
					layersFromAncestor --;
					
				}
				return currentNode;
			}
			currentNode = currentNode.parent;
		}
		return null;
		
	}
	
	public InnerNode getRightSibling() {
		InnerNode currentNode = this.parent;
		int layersFromAncestor = 0;
		while(currentNode != null) {
			layersFromAncestor ++;
			int index = currentNode.getChildIndexByKey(this.entries.get(0).getField());
			if(index < currentNode.children.size() - 1) {
				while(layersFromAncestor > 0) {
					currentNode = (InnerNode)currentNode.children.get(index + 1);
					layersFromAncestor --;
					
				}
				return currentNode;
			}
			currentNode = currentNode.parent;
		}
		return null;
		
	}

	public Node getChildByKey(Field key) {
		int i = 0;
		while(i < this.entries.size()) {
			if(key.compare(RelationalOperator.LTE, this.entries.get(i).getField())) {
				//**If the key is less than the key of entry[i],
				//**	then it could be in the children[i], which is left to the entries[i].
				return this.getChildren().get(i);
			}
			i++;
		}
		return this.getChildren().get(i);
		//**If the key is greater than all of the keys of entries,
		//**	then it could be in the children[i], which is left to the entries[i-1],
		//** here equals to the size of entries.
	}

	public int getChildIndexByKey(Field key) {
		int i = 0;
		while(i < this.entries.size()) {
			if(key.compare(RelationalOperator.LTE, this.entries.get(i).getField())) {
				//**If the key is less than the key of entry[i],
				//**	then it could be in the children[i], which is left to the entries[i].
				return i;
			}
			i++;
		}
		return i;
		//**If the key is greater than all of the keys of entries,
		//**	then it could be in the children[i], which is left to the entries[i-1],
		//** here equals to the size of entries.
	}
	
	public void setChild(int i, Node newChild) {//还没加非法判断
		this.children.set(i, newChild);
	}
	
	public void setEntry(int i, Entry newEntry) {//还没加非法判断
		this.entries.set(i, newEntry);
	}
	
	public void addEntry(Entry newEntry) {
		this.entries.add(newEntry);
	}
	
	public void addChild(Node newChild) {
		this.children.add(newChild);
	}

	public ArrayList<Node> split(){
		ArrayList<Node> newLeafNodes = new ArrayList<Node>();
		InnerNode newLeft = new InnerNode(this.degree, this.parent);
		int i = 0;
		while(i < this.lhe) {
			newLeft.addEntry(this.entries.get(i));
			if(this.children.get(i).isLeafNode()) {
				((LeafNode)this.children.get(i)).setParent(newLeft);
			}
			else{
				((InnerNode)this.children.get(i)).setParent(newLeft);
			}
			newLeft.addChild(this.children.get(i));
			i++;
		}
//		newLeft.setChild(this.degree / 2, this.children.get(this.degree / 2));
		if(this.children.get(i).isLeafNode()) {
			((LeafNode)this.children.get(i)).setParent(newLeft);
		}
		else{
			((InnerNode)this.children.get(i)).setParent(newLeft);
		}
		newLeft.addChild(this.children.get(i));
		newLeafNodes.add(newLeft);

		InnerNode newRight = new InnerNode(this.degree, this.parent);
		i++;
		while(i < this.degree) {
			newRight.addEntry(this.entries.get(i));
			if(this.children.get(i).isLeafNode()) {
				((LeafNode)this.children.get(i)).setParent(newRight);
			}
			else{
				((InnerNode)this.children.get(i)).setParent(newRight);
			}
			newRight.addChild(this.children.get(i));
			i++;
		}
		if(this.children.get(i).isLeafNode()) {
			((LeafNode)this.children.get(i)).setParent(newRight);
		}
		else{
			((InnerNode)this.children.get(i)).setParent(newRight);
		}
		newRight.addChild(this.children.get(i));
		newLeafNodes.add(newRight);
		return newLeafNodes;
	}
	
	public Entry insert(Entry newEntry) {
		Node childToInsert = this.getChildByKey(newEntry.getField());
		if(childToInsert.isLeafNode()) {
			Entry pop = ((LeafNode)childToInsert).insert(newEntry);
			if(pop == null) {
				return null;
			}
			else {//the child inserted is full, pop up a key and split it
				int index = this.children.indexOf(childToInsert);//index where the change happens
				ArrayList<Node> splittedChild = ((LeafNode)childToInsert).split();
				this.entries.add(index, pop);
				this.children.remove(index);
				this.children.add(index, splittedChild.get(1));
				this.children.add(index, splittedChild.get(0));
				
				if(this.entries.size() > this.degree-1){
					return this.entries.get(this.lhe);
				}
				else {
					return null;
				}
			}
		}
		else {
			Entry pop = ((InnerNode)childToInsert).insert(newEntry);
			if(pop == null) {
				return null;
			}
			else {//the child inserted is full, pop up a key and split it
				int index = this.children.indexOf(childToInsert);//index where the change happens
				ArrayList<Node> splittedChild = ((InnerNode)childToInsert).split();
				this.entries.add(index, pop);
				this.children.remove(index);
				this.children.add(index, splittedChild.get(1));
				this.children.add(index, splittedChild.get(0));
				if(this.entries.size() > this.degree-1){
					return this.entries.get(this.lhe);
				}
				else {
					return null;
				}
			}
			
		}
	}
	
	public void setParent(InnerNode parent) {
		this.parent = parent;
	}
	
	public String delete(Entry e) {
		String returnValue = null;
		Node childToDelete = this.getChildByKey(e.getField());
		int index = this.children.indexOf(childToDelete);
//		System.out.println("deleting " +e.getField().toString()+" in "+this.getKeys()+" index="+index);
		if(childToDelete.isLeafNode()) {
			String delResult = ((LeafNode)childToDelete).delete(e);
			if(delResult.equals("not found")) {//not found EXPRESS
				return "not found";
			}
			else {//found and deleted
				if(delResult.equals("do nothing")) {
					returnValue = "do nothing";
					this.replace(e);
					return "do nothing";
				}
				if(delResult.equals("empty")) {
					this.entries.remove(index);
					this.children.remove(index);
					return this.entries.size() < this.minEntries ? "less than half" : "do nothing";
				}
				if(delResult.equals("less than half")){
					this.replace(e);
					LeafNode childLeftSibling = ((LeafNode)childToDelete).getLeftSibling();
					if(childLeftSibling != null) {
						System.out.println("left sibling " + childLeftSibling.getKeys());
						//**it is not the leftmost leaf node
						InnerNode ancestorWithLeft = ((LeafNode)childToDelete).getAncestorWithLeft();
						System.out.println("ancestorWithLeft " + ancestorWithLeft.getKeys());
						if(childLeftSibling.getEntries().size() > childLeftSibling.getMinEntries()) {
							//**childToDelete can borrow from its right sibling
							Entry borrowEntry = childLeftSibling.getMaxEntry(); //extract borrow entry
							ancestorWithLeft.delete(borrowEntry); //delete it from the original place
							((LeafNode)childToDelete).insert(borrowEntry);
							return "do nothing";
						}
						else {//childToDelete can merge to its left sibling
							ArrayList<Entry> mergeEntries = ((LeafNode)childToDelete).getEntries();
							for(Entry mergeEntry: mergeEntries) {
								childLeftSibling.insert(mergeEntry);								
							}
							ancestorWithLeft.entries.set(ancestorWithLeft.getChildIndexByKey(childLeftSibling.getKeys().get(0)), childLeftSibling.getMaxEntry());
							this.children.remove(index);
							this.entries.remove(index);
							return this.entries.size() < this.minEntries ? "less than half" : "do nothing";
						}
					}
					else{//childToDelete not leftmost, then it must have right sibling
						LeafNode childRightSibling = ((LeafNode)childToDelete).getRightSibling();
						InnerNode ancestorWithRight = ((LeafNode)childToDelete).getAncestorWithRight();
						if(childRightSibling.getEntries().size() > childRightSibling.getMinEntries()) {
							//**childToDelete can borrow from its right sibling
							Entry borrowEntry = childRightSibling.getMinEntry(); //extract borrow entry
							ancestorWithRight.delete(borrowEntry); //delete it from the original place
							((LeafNode)childToDelete).insert(borrowEntry);
							return "do nothing";
						}
						else {//childToDelete can merge to its right sibling
							ArrayList<Entry> mergeEntries = ((LeafNode)childToDelete).getEntries();
							for(Entry mergeEntry: mergeEntries) {
								childRightSibling.insert(mergeEntry);								
							}
//							ancestorWithRight.entries.set(ancestorWithRight.getChildIndexByKey(childRightSibling.getKeys().get(0)), childRightSibling.getMaxEntry());
							this.children.remove(index);
							this.entries.remove(index);
							return this.entries.size() < this.minEntries ? "less than half" : "do nothing";
						}
						
					}
					
				}
			}
		}
		else {//children are inner nodes
			String delResult = ((InnerNode)childToDelete).delete(e);
			if(delResult.equals("not found")) {//not found EXPRESS
				return "not found";
			}
			else {//found and deleted
				if(delResult.equals("do nothing")) {
					returnValue = "do nothing";
					this.replace(e);
					return "do nothing";
				}
				if(delResult.equals("empty")) {
					this.entries.remove(index);
					this.children.remove(index);
//					return this.entries.size() < this.minEntries ? "less than half" : "do nothing";
					return this.entries.size() == 0 ? "less than half" : "do nothing";
				}
				if(delResult.equals("less than half")){
//					this.replace(e);
//					InnerNode childLeftSibling = ((InnerNode)childToDelete).getLeftSibling();
//					if(childLeftSibling != null) {
//						//**it is not the leftmost leaf node
//						InnerNode ancestorWithLeft = ((InnerNode)childToDelete).getAncestorWithLeft();
//						if(childLeftSibling.getKeys().size() > childLeftSibling.getMinEntries()) {
//							//**childToDelete can borrow from its right sibling
//							Entry borrowEntry = childLeftSibling.getMaxEntry(); //extract borrow entry
//							ancestorWithLeft.delete(borrowEntry); //delete it from the original place
//							((LeafNode)childToDelete).insert(borrowEntry);
//						}
//						else {//**childToDelete can merge to its right sibling
//							//merge
//						}
//					}
//					else{//not leftmost
//						LeafNode childRightSibling = ((LeafNode)childToDelete).getRightSibling();
//						InnerNode ancestorWithRight = ((LeafNode)childToDelete).getAncestorWithRight();
//						if(childRightSibling.getEntries().size() > childRightSibling.getMinEntries()) {
//							//**childToDelete can borrow from its right sibling
//							Entry borrowEntry = childRightSibling.getMinEntry(); //extract borrow entry
//							ancestorWithRight.delete(borrowEntry); //delete it from the original place
//							((LeafNode)childToDelete).insert(borrowEntry);
//						}
//						else {//**childToDelete can merge to its right sibling
//							//merge
//						}
//					}
						return "no nothing";
					
				}
			}
			
		}
		return null;
	}
	
	public void replace(Entry e) {//replace e if e is in this inner node
		int index = -1;
		for(int i = 0; i < this.entries.size(); i++) {
			if(this.entries.get(i).getField().compare(RelationalOperator.EQ, e.getField())) {
				index = i;
			}
		}
		if(index == -1) {//e not in this inner node
			return;
		}
		//*replaced by the max entry in the left subtree of that place
		this.entries.set(index, this.getMaxLeaf(this.children.get(index)).getMaxEntry());
	}
	
	public LeafNode getMaxLeaf(Node child) {
		if(child.isLeafNode()) {
			return ((LeafNode)child);
		}
		else {
//			return ((InnerNode)this.children.get(this.children.size() - 1)).getMaxLeaf();
			return ((InnerNode)child).getMaxLeaf(((InnerNode)child).children.get(((InnerNode)child).children.size() - 1));
			//*do getMaxLeaf recursively to the last subtree of child
		}
	}

//	public LeafNode getMinLeaf() {
//		if(this.children.get(this.children.size() - 1).isLeafNode()) {
//			return ((LeafNode)this.children.get(this.children.size() - 1));
//		}
//		else {
//			return ((InnerNode)this.children.get(this.children.size() - 1)).getMinLeaf();
//		}
//	}
	
	public LeafNode getMinLeaf(Node child) {
		if(child.isLeafNode()) {
			return ((LeafNode)child);
		}
		else {
//			return ((InnerNode)this.children.get(this.children.size() - 1)).getMaxLeaf();
			return ((InnerNode)child).getMinLeaf(((InnerNode)child).children.get(0));
			//*do getMinLeaf recursively to the first subtree of child
		}
	}
	
	
	
//	public void insert(Field key) {
//		Node childToInsert = this.getChildByKey(key);
//		if(childToInsert.isLeafNode()) {
//			LeafNode leafToInsert = new LeafNode()
//		}
//		childToInsert.insert(key);
//	}

}