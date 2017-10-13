package Q2bpkg12;


import java.util.Comparator;

public class MyNOPComparator implements Comparator<NOP> {

	public int compare(NOP e1, NOP e2) {
		 
        if(e1.getSum()>e2.getSum()){
 
            return 1;
 
        } else {
 
            return -1;
 
        }
 
    }
	
}
