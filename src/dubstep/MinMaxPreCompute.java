package dubstep;

import java.util.ArrayList;
import java.util.HashMap;

public class MinMaxPreCompute {

	ArrayList<String> thisList;
	double computedVal;

	double maxValue; 
	HashMap<String,Double> maxList;

	double minValue; 
	HashMap<String,Double> minList;

	MinMaxPreCompute(ArrayList<String> incoming){
		thisList=new ArrayList<String>();
		thisList.addAll(incoming);
		maxList=new HashMap<String,Double>();
		minList=new HashMap<String,Double>();
	}

	void setMinMax(ArrayList<Double> incomingValue){
		computedVal=computeSum(incomingValue);
		setMax(incomingValue);
		setMin(incomingValue);

	}

	void setMax(ArrayList<Double> incomingMax){
		if(maxList.size()==0) {
			maxValue=computedVal;
		}
		if(maxList.size()==0 || computedVal>maxValue){
			maxValue=computedVal;
			maxList.clear();
			for(int i=0;i<incomingMax.size();i++){
				double  tempList; 
				tempList=incomingMax.get(i);
				maxList.put(thisList.get(i), tempList);
			}
		}
		else if(computedVal==maxValue){
			for(int i=0;i<incomingMax.size();i++){
				double  tempList=Math.max(maxList.get(thisList.get(i)), incomingMax.get(i));
				maxList.put(thisList.get(i), tempList);
			}
		}
		
	}


	void setMin(ArrayList<Double> incomingMin){
		if(minList.size()==0) {
			minValue=computedVal;
		}
		if(minList.size()==0 || computedVal<minValue){
			minValue=computedVal;
			minList.clear();
			for(int i=0;i<incomingMin.size();i++){
				double tempList=incomingMin.get(i);
				minList.put(thisList.get(i), tempList);
			}
		}
		else if(computedVal==minValue){
			for(int i=0;i<incomingMin.size();i++){
				double  tempList=Math.min(minList.get(thisList.get(i)), incomingMin.get(i));
				minList.put(thisList.get(i), tempList);
			}
		}
	}

	double computeSum(ArrayList<Double> incomingValue){
		double tempSum=0;
		for(double each : incomingValue)
			tempSum+=each;
		return tempSum;
	}


}
