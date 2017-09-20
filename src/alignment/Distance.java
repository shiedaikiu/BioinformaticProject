/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package alignment;

public class Distance {
    
    public static void main(String[] args) {
        int d=getLevenshteinDistance("ACGTT", "GT");
    }
    
    public static int getHammingDistance(String d1, String d2) {
        int distance = 0;
        if (d1 == null || d2 == null) {
            System.out.println("Either String is null");
            System.exit(0); // or throw a RuntimeException here
        }

        d1 = d1.toUpperCase();
        d2 = d2.toUpperCase();

        if (d1.length() != d2.length()) {
            System.out.println("The strings are not equal in length.");
            System.exit(0); //or throw a RuntimeException here
        }

        for (int i = 0; i < d1.length(); i++) {
            if (d1.charAt(i) != d2.charAt(i)) {
                distance++;
            }
        }

        return distance;
    }
    public static int getLevenshteinDistance(String s, String t) {
      if (s == null || t == null) {
          throw new IllegalArgumentException("Strings must not be null");
      }

      /*
         The difference between this impl. and the previous is that, rather 
         than creating and retaining a matrix of size s.length()+1 by t.length()+1, 
         we maintain two single-dimensional arrays of length s.length()+1.  The first, d,
         is the 'current working' distance array that maintains the newest distance cost
         counts as we iterate through the characters of String s.  Each time we increment
         the index of String t we are comparing, d is copied to p, the second int[].  Doing so
         allows us to retain the previous cost counts as required by the algorithm (taking 
         the minimum of the cost count to the left, up one, and diagonally up and to the left
         of the current cost count being calculated).  (Note that the arrays aren't really 
         copied anymore, just switched...this is clearly much better than cloning an array 
         or doing a System.arraycopy() each time  through the outer loop.)

         Effectively, the difference between the two implementations is this one does not 
         cause an out of memory condition when calculating the LD over two very large strings.
       */

      int n = s.length(); // length of s
      int m = t.length(); // length of t

      if (n == 0) {
          return m;
      } else if (m == 0) {
          return n;
      }

      if (n > m) {
          // swap the input strings to consume less memory
          String tmp = s;
          s = t;
          t = tmp;
          n = m;
          m = t.length();
      }

      int p[] = new int[n+1]; //'previous' cost array, horizontally
      int d[] = new int[n+1]; // cost array, horizontally
      int _d[]; //placeholder to assist in swapping p and d

      // indexes into strings s and t
      int i; // iterates through s
      int j; // iterates through t

      char t_j; // jth character of t

      int cost; // cost

      for (i = 0; i<=n; i++) {
          p[i] = i;
      }

      for (j = 1; j<=m; j++) {
          t_j = t.charAt(j-1);
          d[0] = j;

          for (i=1; i<=n; i++) {
              cost = s.charAt(i-1)==t_j ? 0 : 1;
              // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
              d[i] = Math.min(Math.min(d[i-1]+1, p[i]+1),  p[i-1]+cost);
          }

          // copy current distance counts to 'previous row' distance counts
          _d = p;
          p = d;
          d = _d;
      }

      // our last action in the above loop was to switch d and p, so p now 
      // actually has the most recent cost counts
      return p[n];
  }
}
