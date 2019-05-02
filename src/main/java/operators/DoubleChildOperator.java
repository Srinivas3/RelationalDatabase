package operators;

public interface DoubleChildOperator {
    public Operator getLeftChild();

    public Operator getRightChild();

    public void setLeftChild(Operator leftChild);

    public void setRightChild(Operator rightChild);
}
