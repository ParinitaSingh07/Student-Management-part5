import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Student Management Application
 * - OOP: interfaces, abstraction, inheritance
 * - Operations: add, update, delete, search, view
 * - Persistent storage: CSV file (students_db.csv)
 * - Exception handling with custom exceptions
 * - Multithreading to simulate loading/saving operations
 * - Java Collections Framework usage (HashMap + ArrayList)
 * - Sorting by marks using Comparator
 *
 * To compile: javac StudentManagementApp.java
 * To run:     java StudentManagementApp
 */

// -------------------- Custom Exceptions --------------------
class StudentException extends Exception {
    public StudentException(String msg) { super(msg); }
}

class StudentNotFoundException extends StudentException {
    public StudentNotFoundException(String msg) { super(msg); }
}

class InvalidInputException extends StudentException {
    public InvalidInputException(String msg) { super(msg); }
}

// -------------------- OOP: Abstraction & Inheritance --------------------
abstract class Person implements Serializable {
    private static final long serialVersionUID = 1L;
    protected Integer id; // wrapper Integer used
    protected String name;

    public Person(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() { return id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public abstract String getDetails();
}

class Student extends Person {
    private static final long serialVersionUID = 1L;
    private Double marks; // wrapper Double

    public Student(Integer id, String name, Double marks) {
        super(id, name);
        this.marks = marks;
    }

    public Double getMarks() { return marks; }
    public void setMarks(Double marks) { this.marks = marks; }

    @Override
    public String getDetails() {
        return String.format("%d,%s,%.2f", id, name, marks);
    }

    @Override
    public String toString() {
        return String.format("Student{id=%d, name='%s', marks=%.2f}", id, name, marks);
    }
}

// -------------------- Interfaces --------------------
interface StudentRepository {
    void addStudent(Student s) throws InvalidInputException;
    void updateStudent(Student s) throws StudentNotFoundException, InvalidInputException;
    void deleteStudent(Integer id) throws StudentNotFoundException;
    Student findById(Integer id) throws StudentNotFoundException;
    List<Student> listAll();
    void load() throws IOException;
    void save() throws IOException;
}

// -------------------- Implementation --------------------
class FileBackedStudentRepository implements StudentRepository {
    private final Map<Integer, Student> students = new ConcurrentHashMap<>();
    private final String filename;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public FileBackedStudentRepository(String filename) {
        this.filename = filename;
    }

    // Validation logic
    private void validateStudent(Student s, boolean checkDuplicate) throws InvalidInputException {
        if (s == null) throw new InvalidInputException("Student object is null");
        if (s.getId() == null || s.getId() <= 0) throw new InvalidInputException("ID must be positive integer");
        if (s.getName() == null || s.getName().trim().isEmpty()) throw new InvalidInputException("Name cannot be empty");
        if (s.getMarks() == null || s.getMarks() < 0.0 || s.getMarks() > 100.0) throw new InvalidInputException("Marks must be between 0 and 100");
        if (checkDuplicate && students.containsKey(s.getId())) throw new InvalidInputException("Duplicate student ID: " + s.getId());
    }

    @Override
    public void addStudent(Student s) throws InvalidInputException {
        validateStudent(s, true);
        students.put(s.getId(), s);
    }

    @Override
    public void updateStudent(Student s) throws StudentNotFoundException, InvalidInputException {
        if (s == null) throw new InvalidInputException("Student is null");
        Integer id = s.getId();
        if (!students.containsKey(id)) throw new StudentNotFoundException("Student not found with ID: " + id);
        validateStudent(s, false);
        students.put(id, s);
    }

    @Override
    public void deleteStudent(Integer id) throws StudentNotFoundException {
        if (id == null || !students.containsKey(id)) throw new StudentNotFoundException("Student not found with ID: " + id);
        students.remove(id);
    }

    @Override
    public Student findById(Integer id) throws StudentNotFoundException {
        Student s = students.get(id);
        if (s == null) throw new StudentNotFoundException("Student not found with ID: " + id);
        return s;
    }

    @Override
    public List<Student> listAll() {
        return new ArrayList<>(students.values());
    }

    // load from CSV file; simulates a loading delay on a background thread
    @Override
    public void load() throws IOException {
        File file = new File(filename);
        if (!file.exists()) {
            // create empty file
            file.createNewFile();
            return;
        }

        // simulate loading progress using a separate thread
        CountDownLatch latch = new CountDownLatch(1);
        executor.submit(() -> {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                students.clear();
                int count = 0;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split(",");
                    // defensive parsing
                    try {
                        Integer id = Integer.valueOf(parts[0].trim());
                        String name = parts[1].trim();
                        Double marks = Double.valueOf(parts[2].trim());
                        Student s = new Student(id, name, marks);
                        students.put(id, s);
                        count++;
                        // small sleep to simulate progressive loading
                        Thread.sleep(80);
                    } catch (Exception e) {
                        // skip malformed lines
                    }
                }
                System.out.println("[Loader] Loaded " + count + " records from " + filename);
            } catch (IOException | InterruptedException e) {
                System.err.println("[Loader] Error: " + e.getMessage());
            } finally {
                latch.countDown();
            }
        });

        try {
            // wait for background loading to finish but with timeout
            if (!latch.await(5, TimeUnit.SECONDS)) {
                System.out.println("[Loader] Loading taking longer than expected; continuing...");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // save synchronously; demonstrates try-with-resources and exception handling
    @Override
    public void save() throws IOException {
        File tmp = new File(filename + ".tmp");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmp))) {
            for (Student s : students.values()) {
                bw.write(s.getDetails());
                bw.newLine();
            }
            bw.flush();
        } catch (IOException e) {
            throw new IOException("Failed writing to temp file: " + e.getMessage(), e);
        }
        // atomic replace
        File original = new File(filename);
        if (tmp.renameTo(original)) {
            System.out.println("[Saver] Saved " + students.size() + " records to " + filename);
        } else {
            // fallback copy
            try (InputStream in = new FileInputStream(tmp); OutputStream out = new FileOutputStream(original)) {
                byte[] buf = new byte[8192];
                int len;
                while ((len = in.read(buf)) > 0) out.write(buf, 0, len);
                System.out.println("[Saver] Saved (fallback) " + students.size() + " records to " + filename);
            }
            tmp.delete();
        }
    }

    public void shutdown() {
        executor.shutdownNow();
    }
}

// -------------------- Application UI --------------------
public class StudentManagementApp {
    private static final Scanner scanner = new Scanner(System.in);
    private static final StudentRepository repo = new FileBackedStudentRepository("students_db.csv");

    public static void main(String[] args) {
        System.out.println("Starting Student Management Application...");
        try {
            repo.load();
        } catch (IOException e) {
            System.err.println("Error during initial load: " + e.getMessage());
        }

        boolean running = true;
        while (running) {
            showMenu();
            String choice = scanner.nextLine().trim();
            switch (choice) {
                case "1": handleAdd(); break;
                case "2": handleUpdate(); break;
                case "3": handleDelete(); break;
                case "4": handleSearch(); break;
                case "5": handleViewAll(); break;
                case "6": handleSortByMarks(); break;
                case "0":
                    handleExit();
                    running = false; break;
                default: System.out.println("Invalid option. Try again.");
            }
        }
    }

    private static void showMenu() {
        System.out.println("\n=== MENU ===");
        System.out.println("1. Add Student");
        System.out.println("2. Update Student");
        System.out.println("3. Delete Student");
        System.out.println("4. Search Student by ID");
        System.out.println("5. View All Students");
        System.out.println("6. View Students Sorted by Marks");
        System.out.println("0. Exit and Save");
        System.out.print("Choose: ");
    }

    private static void handleAdd() {
        try {
            System.out.print("Enter ID: ");
            Integer id = Integer.valueOf(scanner.nextLine().trim());
            System.out.print("Enter name: ");
            String name = scanner.nextLine().trim();
            System.out.print("Enter marks (0-100): ");
            Double marks = Double.valueOf(scanner.nextLine().trim());

            Student s = new Student(id, name, marks);
            repo.addStudent(s);
            System.out.println("Student added: " + s);
        } catch (NumberFormatException nfe) {
            System.err.println("Invalid numeric input.");
        } catch (InvalidInputException iie) {
            System.err.println("Validation failed: " + iie.getMessage());
        }
    }

    private static void handleUpdate() {
        try {
            System.out.print("Enter ID to update: ");
            Integer id = Integer.valueOf(scanner.nextLine().trim());
            Student existing = repo.findById(id);
            System.out.println("Existing: " + existing);

            System.out.print("New name (leave blank to keep): ");
            String name = scanner.nextLine();
            if (!name.trim().isEmpty()) existing.setName(name.trim());

            System.out.print("New marks (leave blank to keep): ");
            String marksStr = scanner.nextLine().trim();
            if (!marksStr.isEmpty()) existing.setMarks(Double.valueOf(marksStr));

            repo.updateStudent(existing);
            System.out.println("Student updated: " + existing);
        } catch (NumberFormatException nfe) {
            System.err.println("Invalid numeric input.");
        } catch (StudentNotFoundException snf) {
            System.err.println(snf.getMessage());
        } catch (InvalidInputException iie) {
            System.err.println("Validation failed: " + iie.getMessage());
        }
    }

    private static void handleDelete() {
        try {
            System.out.print("Enter ID to delete: ");
            Integer id = Integer.valueOf(scanner.nextLine().trim());
            repo.deleteStudent(id);
            System.out.println("Student deleted: ID=" + id);
        } catch (NumberFormatException nfe) {
            System.err.println("Invalid numeric input.");
        } catch (StudentNotFoundException snf) {
            System.err.println(snf.getMessage());
        }
    }

    private static void handleSearch() {
        try {
            System.out.print("Enter ID to search: ");
            Integer id = Integer.valueOf(scanner.nextLine().trim());
            Student s = repo.findById(id);
            System.out.println("Found: " + s);
        } catch (NumberFormatException nfe) {
            System.err.println("Invalid numeric input.");
        } catch (StudentNotFoundException snf) {
            System.err.println(snf.getMessage());
        }
    }

    private static void handleViewAll() {
        List<Student> list = repo.listAll();
        if (list.isEmpty()) {
            System.out.println("No students available.");
            return;
        }
        System.out.println("\n--- All Students (Iterator) ---");
        Iterator<Student> it = list.iterator();
        while (it.hasNext()) System.out.println(it.next());
    }

    private static void handleSortByMarks() {
        List<Student> list = repo.listAll();
        list.sort(Comparator.comparingDouble(Student::getMarks).reversed()); // high to low
        System.out.println("\n--- Students Sorted by Marks (High -> Low) ---");
        for (Student s : list) System.out.println(s);
    }

    private static void handleExit() {
        System.out.println("Saving data before exit...");
        try {
            repo.save();
        } catch (IOException e) {
            System.err.println("Failed to save data: " + e.getMessage());
        }

        // If repository has resources, attempt shutdown
        if (repo instanceof FileBackedStudentRepository) {
            ((FileBackedStudentRepository) repo).shutdown();
        }

        System.out.println("Goodbye.");
    }
}
