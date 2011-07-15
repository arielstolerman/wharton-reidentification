package other;

import java.util.*;

public class IDComparator<T> implements Comparator<T> {

	Map<String,Integer> indexMap;
	
	public IDComparator(Map<String,Integer> indexMap) {
		this.indexMap = indexMap;
	}
	
	@Override
	public int compare(T arg0, T arg1) {
		return indexMap.get(arg0.toString()) - indexMap.get(arg1.toString());
	}

}
