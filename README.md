# SparkReduceTrick

**Output:**  
[5]=[ggg, fff]  
[1, 2]=[aaa, ccc, bbb]  
[3]=[eee, ddd]  
[4, 5]=[eee, ggg, fff]  
[4, 3]=[eee, ddd, fff]  
[2]=[ccc, bbb]  
[6]=[zzz]  
[1]=[aaa, bbb]  



**Output1:**  
[1, 2]=[aaa, ccc, bbb]  
[4, 3]=[eee, ddd, fff]  
[4, 5]=[eee, ggg, fff]  
[6]=[zzz]  



**Output2:**  
[1, 2]=[aaa, ccc, bbb]  
[4, 5, 3]=[eee, ddd, ggg, fff]  
[6]=[zzz]  