import java.util.HashMap;
import java.util.Random;

/**
 * Created by faizan on 10/30/16.
 */
public class test {
    public static void main(String[] args){

        String a = "we all we all we all";
        String b = "rock";

        //weAllRock(a,b);
        int[][] M=new int[][]{{0, 1, 0}, {0, 0, 0}, {0, 1, 0}};//{{0, 0, 1,1}, {1, 0, 0,1}, {0, 0, 0,1},{0, 0, 0,1}};
        int r=getId(M,3);
        System.out.println("result "+r);



    }
    public static boolean weAllRock(String a, String b){

        int i=0;
        String result="";
        int j=0;
        while (i<a.length()){
            if (j==7) {
                result = result + b+" ";
                j=0;
            }
            result=result+a.charAt(i);
            j++;
            i++;
        }
        System.out.println(result);
        return true;
    }

    public static int getId(int M[][], int n)
    {
        System.out.println("here");
        // Your code here
        /*for(int i=0;i<n;){
            for(int j=0;j<n;){
                if(i!=j){
                    if(M[i][j]==1){
                        i++;
                        if(i==n&&j==n){
                            return -1;
                        }
                    }else
                        if (j==n)
                            return -1;
                        j++;
                }
            }
        }
        return 1;*/
        int i=0;
        int j=0;
        int count=1;
        do {
            if (i != j) {
                if (M[j][i]==1){

                    System.out.println(j+" "+i);
                    j++;
                    count++;
                    if (count==n+1)
                        return i+1;
                }else
                {
                    j=0;
                    count=1;
                    i++;
                }
            }
            else if (j==n){
                i++;
                j=0;
                count=1;
            }
            else if (i==j){
                j++;
                count++;
            }
            //else
              //  System.out.println("this case");
        }while (j<=n&&i<=n);
        return -1;
    }
}
