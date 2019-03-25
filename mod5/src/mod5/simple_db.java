package mod5;

//import java.sql.*;


import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.lang.String;


//import com.opencsv.CSVReader;

public class simple_db{  
public static void main(String args[])throws IOException{  
try{
	
	CSVReader reader = new CSVReader(new FileReader("w.csv"), '\t');

	//List<Employee> emps = new ArrayList<Employee>();

	// read line by line
	String[] record = null;
	String[] value1=null;
	List<String[]> myEntries = reader.readAll();
	int arg =246-3;
	if(arg<0){System.out.println("invalid input");}
	
	int i=0;
	String[][] beating = new String[250][250];
	String[][] beaten = new String[250][250];

	for (String[] j:myEntries){ beating[i] = j[1].split(",");   beaten[i++] = j[2].split(","); }  //{beating[i]=j[1].split(","); beating[i++]=j[2];}
	for(i=0;i<2;i++){arg++; for(int e=0;e<beating[0].length;e++)System.out.println(beating[arg][e]);}
	//for (int w=0;w<beating.length;w++)System.out.println(beating[w][0]);
	//System.out.println(beating[arg][0]);
	while ((record = reader.readNext()) != null) {
		//System.out.println(record[1]);
		value1 = record[1].split(",");
		//if (arg==record[0]){}
		//for(i=0; i<value1.length;i++)System.out.println224(value1[i]);//{value1 = record[1].split(",");System.out.println(value1[1]);}
		//value1 = record[1].split(",");
		//for(String i:record[1].split(",")){ if(i!=null)System.out.println(i); }// else System.exit(0); }
		//System.out.println(value1[0]);
	}
	//System.out.println(record[arg]);
}
catch(Exception e)
{
	System.out.println("lol");
	System.out.println(e);
}  

}  
}  