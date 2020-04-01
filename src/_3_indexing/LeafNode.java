package _3_indexing;

import java.util.ArrayList;

import _1_basic.Field;
import _1_basic.RelationalOperator;

public class LeafNode implements Node {
	int degree;
	ArrayList<Entry> entries;
	InnerNode parent;
	int lhe;//left half end
	int minEntries;
	
	public LeafNode(int degree, InnerNode parent) {
		//your code here
		this.degree = degree;
		this.entries = new ArrayList<Entry>();
		this.parent = parent;
		this.lhe = (degree) / 2;
		this.minEntries = (degree + 1) / 2;
//		this.lhe = this.degree % 2 == 0 ? (degree-1)/2 : (degree/2);
		
		
		
		
//		this.lhe = Math.round((float)degree/2);
	}
	
	public ArrayList<Entry> getEntries() {
		//your code here
		return this.entries;
	}

	public int getDegree() {
		//your code here
		return this.degree;
	}
	
	public int getMinEntries() {
		return this.minEntries;
	}
	
	public ArrayList<Field> getKeys() {
		//your code here
		ArrayList<Field> keys = new ArrayList<Field>();
		for(Entry entry: this.entries) {
			keys.add(entry.getField());
		}
		return keys;
	}
	
	public boolean isLeafNode() {
		return true;
	}
	
	public boolean contains(Field key) {
		for(Entry entry: this.entries) {
			if(key.compare(RelationalOperator.EQ, entry.getField())){
				return true;
			}
		}
		return false;
	}
	
	public InnerNode getParent() {
		return this.parent;
	}
//
//	public LeafNode getLeftSibling_LeafNode() {//another version
//		int i = this.parent.getChildren().indexOf(this);
//		if(i == 0) {//this node is the leftmost one
//			return null;//it has no left sibling
//		}
//		else {
//			return (LeafNode) this.parent.getChildren().get(i - 1);
//		}
//	}
	
//	public Node getLeftSibling() {
//		int i = this.parent.getChildren().indexOf(this);
//		if(i == 0) {//this node is the leftmost one
//			return null;//it has no left sibling
//		}
//		else {
//			return this.parent.getChildren().get(i - 1);
//		}
//	}
//	
//	public Node getRightSibling() {
//		int i = this.parent.getChildren().indexOf(this);
//		if(i == this.parent.degree) {//this node is the rightmost one
//			return null;//it has no left sibling
//		}
//		else {
//			return this.parent.getChildren().get(i + 1);
//		}
//	}
	
	public Entry insert(Entry newEntry) {
		boolean inserted = false;
		for(int i = 0; i < this.entries.size(); i++) {//find the position to insert into
			if(newEntry.getField().compare(RelationalOperator.LT, entries.get(i).getField())) {
				this.entries.add(i, newEntry);
				inserted = true;
				break;
			}
		}
		if(!inserted) {//if newEntry is greater than every current entries, insert to the tail
			this.entries.add(newEntry);
		}
		//*check if the capacity is exceeded
		if(this.entries.size() > this.degree) {
//			System.out.println("from "+this.getKeys()+" pop "+ this.entries.get(this.lhe).getField());
			return this.entries.get(this.lhe);
		}
		else {
			return null;
		}
	}
	
	public ArrayList<Node> split(){
		ArrayList<Node> newLeafNodes = new ArrayList<Node>();
		LeafNode newLeft = new LeafNode(this.degree, this.parent);
		for(int i = 0; i <= lhe; i++) {
			newLeft.insert(this.getEntries().get(i));
		}
		newLeafNodes.add(newLeft);
		LeafNode newRight = new LeafNode(this.degree, this.parent);
		for(int i = this.lhe + 1; i <= this.degree; i++) {
			newRight.insert(this.getEntries().get(i));
		}
		newLeafNodes.add(newRight);
		return newLeafNodes;
	}
	
	public void setParent(InnerNode parent) {
		this.parent = parent;
	}
	
	public String delete(Entry e) {
		if(!this.contains(e.getField())) {
			return "not found";
		}
		int index = 0;
		while(index < this.entries.size()) {
			if(this.entries.get(index).getField().equals(e.getField())) {
				this.entries.remove(index);
				break;
			}
			index++;
		}
//		System.out.println("deleting " +e.getField().toString()+" in "+this.getKeys()+" index="+index);
		if(this.entries.size() == 0) {
			return "empty";
		}
		if(this.entries.size() < this.minEntries ) {
			return "less than half";
		}
		return "do nothing";
	}
	
	public void borrow(LeafNode other, boolean fromLeft) {
		
	}
	
	public LeafNode merge(LeafNode other) {
		return null;
	}

	public Entry getMaxEntry() {
		return this.entries.get(this.entries.size() - 1);
	}
	
	public Entry getMinEntry() {
		return this.entries.get(0);
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
	
	public LeafNode getLeftSibling() {
		InnerNode currentNode = this.parent;
		while(currentNode != null) {
			int index = currentNode.getChildIndexByKey(this.entries.get(0).getField());
			if(index != 0) {
				return currentNode.getMaxLeaf(currentNode.children.get(index - 1));
			}
			currentNode = currentNode.parent;
		}
		return null;
		
	}
	
	public LeafNode getRightSibling() {
		InnerNode currentNode = this.parent;
		while(currentNode != null) {
			int index = currentNode.getChildIndexByKey(this.entries.get(0).getField());
//			System.out.println(index);
			if(index < currentNode.children.size() - 1) {
				return currentNode.getMinLeaf(currentNode.children.get(index + 1));
			}
			currentNode = currentNode.parent;
		}
		return null;
		
	}
}