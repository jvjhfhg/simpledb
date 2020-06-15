package simpledb;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets;
    private int min;
    private int max;

    private int[] lowerbound;
    private int[] width;
    private int[] cnt;
    private int size;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	this.lowerbound = new int[buckets];
    	this.width = new int[buckets];
    	this.cnt = new int[buckets];
    	this.size = 0;

    	int total = max - min + 1;
    	int cap = total / buckets;
    	int remainder = total % buckets;

    	int l = min;
    	for (int i = 0; i < buckets; ++i) {
            this.lowerbound[i] = l;
            this.cnt[i] = 0;

            this.width[i] = cap;
            if (i < remainder) {
                this.width[i]++;
            }

            l += this.width[i];
        }
    }

    /**
     * Calculate the index of bucket which v is in.
     * @param v Value whose bucket needs to be calculated
     * @return The index of specified bucket
     */
    private int calcBucket(int v) {
        if (v < min || v > max) {
            return -1;
        }
        int res = 0, l = 1, r = buckets - 1;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (lowerbound[mid] <= v) {
                res = mid;
                l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        return res;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int b = calcBucket(v);
        if (b != -1) {
            cnt[b]++;
            size++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int b = calcBucket(v);

        double res = 0;

        switch (op) {
            case EQUALS:
                if (b == -1) {
                    return 0;
                }
                return (double) cnt[b] / width[b] / size;
            case GREATER_THAN_OR_EQ:
            case GREATER_THAN:
                if (v < min) {
                    return 1;
                }
                if (v > max) {
                    return 0;
                }
                for (int i = b + 1; i < buckets; ++i) {
                    res += cnt[i];
                }
                res += (double) cnt[b] * (lowerbound[b] + width[b] - 1 - v) / width[b];
                if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
                    res += (double) cnt[b] / width[b];
                }

                return res / size;
            case LESS_THAN_OR_EQ:
            case LESS_THAN:
                if (v > max) {
                    return 1;
                }
                if (v < min) {
                    return 0;
                }
                for (int i = b - 1; i >= 0; --i) {
                    res += cnt[i];
                }
                res += (double) cnt[b] * (v - lowerbound[b]) / width[b];
                if (op == Predicate.Op.LESS_THAN_OR_EQ) {
                    res += (double) cnt[b] / width[b];
                }

                return res / size;
            case NOT_EQUALS:
                if (v < min || v > max) {
                    return 1;
                }
                return 1 - (double) cnt[b] / width[b] / size;
            default:
                return -1;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        throw new NotImplementedException();
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < buckets; ++i) {
            stringBuilder.append(String.format("[%d, %d] = %d\n", lowerbound[i], lowerbound[i] + width[i] - 1, cnt[i]));
        }
        return stringBuilder.toString();
    }
}
