import java.util.Random;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.caliper.api.VmParam;

/**
 * Run it like so:
 *      java EscapeAnalysisBenchmark -Jescape=-XX:-DoEscapeAnalysis,-XX:+DoEscapeAnalysis
 */
public class EscapeAnalysisBenchmark extends SimpleBenchmark {

    // @VmParam seems not work, command line option such as:
    //      -Jescape=-XX:-DoEscapeAnalysis,-XX:+DoEscapeAnalysis
    // does work though.
	// @VmParam({"-XX:-DoEscapeAnalysis", "-XX:+DoEscapeAnalysis"}) String escape;

    @Param({"1000",   "2000",   "3000",   "4000",   "5000",
            "10000",  "15000",  "20000",  "25000",  "30000",
            "40000",  "50000",  "60000",  "70000",  "80000",
            "100000", "150000", "200000", "250000", "300000"}) private VectorList normals;

    private VectorList tan;
    private VectorList cot;

    @Override protected void setUp() throws Exception {
        tan = new VectorList(normals.size());
        cot = new VectorList(normals.size());
    }

	public void timeCacheFriendly(int reps) {
		for (int i = 0; i < reps; ++i) {
            for (int a=0; a<normals.size();a++) {
                Vector n = normals.get(a); // alloc
                Vector ta = tan.get(a); // alloc
                float dot=n.dot(ta);
                n = n.scale(- dot);   // alloc
                ta = ta.add(n);       // alloc
                ta = ta.normalize();  // alloc
                tan.set(a,ta);
                n = Vector.cross(normals.get(a),ta); // 2x alloc
                float w =  (n.dot(cot.get(a)) < 0.0f)
                            ? -1.0f : 1.0f; // alloc
                n = n.scale(w);     // alloc
                n = n.normalize();  // alloc
                cot.set(a,n);
            }
        }
	}

    public void timeCacheUnfriendly(int reps) {
        for (int i = 0; i < reps; ++i) {
            for (int a=0; a<normals.size();a++) {
                Vector n = normals.get2(a);
                Vector ta = tan.get2(a);
                float dot=n.dot(ta);
                n = n.scale(- dot);   // alloc
                ta = ta.add(n);       // alloc
                ta = ta.normalize();  // alloc
                tan.set2(a,ta);
                n = Vector.cross(normals.get2(a),ta); // alloc
                float w =  (n.dot(cot.get2(a)) < 0.0f)
                            ? -1.0f : 1.0f;
                n = n.scale(w);     // alloc
                n = n.normalize();  // alloc
                cot.set2(a,n);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Runner.main(EscapeAnalysisBenchmark.class, args);
    }
}

class Vector {

    public final float x, y, z;

    public Vector(float x, float y, float z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public float dot(Vector v) {
        return x*v.x + y*v.y + z*v.z;
    }

    public Vector scale(float s) {
        return new Vector(x*s, y*s, z*s);
    }

    public Vector add(Vector v)
    {
        return new Vector(x + v.x, y + v.y, z+v.z);
    }

    public Vector normalize() {
        float norm = (float)
                        (1.0/Math.sqrt(x*x +
                                        y*y + z*z));
        return new Vector(x*norm, y*norm, z*norm);
    }

    public static Vector cross(Vector v, Vector w) {
        float x = v.y*w.z - v.z*w.y;
        float y = w.x*v.z - w.z*v.x;
        float z = v.x*w.y - v.y*w.x;
        return new Vector(x, y, z);
    }
}

class VectorList {
    private float[] data;
    private int size;
    private Vector[] vectors;

    private static final int DIMENSION = 1024;

    public VectorList(int size) {
        data = new float[size*3];
        vectors = new Vector[size];
        this.size = size;

        Random generator = new Random();
        for (int i = 0; i < size*3; i++) {
            data[i] = generator.nextFloat() * DIMENSION;
        }
        for (int i = 0; i < size; i++) {
            vectors[i] = new Vector(data[i*3], data[i*3+1], data[i*3+2]);
        }
    }

    public Vector get(int index) {
        return new Vector(data[index*3],
                        data[index*3+1],
                        data[index*3+2]);
    }

    public void set(int index, Vector v) {
        data[index*3] = v.x;
        data[index*3+1] = v.y;
        data[index*3+2] = v.z;
    }

    public Vector get2(int index) {
        return vectors[index];
    }

    public void set2(int index, Vector v) {
        vectors[index] = v;
    }

    public int size() { return size; }

    public static VectorList valueOf(String size) {
        return new VectorList(Integer.parseInt(size));
    }
}
