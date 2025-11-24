# 📘 Student Record Management System

A complete **Java-based console application** to manage student records with support for:

* Adding, deleting, updating, and viewing student details
* Searching and sorting
* File handling (Persistent storage using `students.txt`)
* Multithreading (Loading simulation)
* Exception handling & validation
* OOP principles: Abstraction, Inheritance, Interfaces

This project is created as per the **Lab Assignment 5** requirements.

---

## 🚀 Features

### ✔ Object-Oriented Design

* `Person` (abstract class)
* `Student` (extends Person)
* `RecordActions` interface
* `StudentManager` implementing RecordActions

### ✔ File Handling

* Stores data in `students.txt`
* Uses `BufferedReader` and `BufferedWriter`
* Automatically loads existing records on startup

### ✔ Multithreading

* Custom `Loader` thread simulates loading animation

### ✔ Sorting & Searching

* Sorts students by marks (descending)
* Searches by name
* Displays using Iterator

### ✔ Custom Exception

* `StudentNotFoundException` for invalid search/delete operations

---

## 📂 Project Structure

```
StudentRecordSystem.java  (Single-file implementation)
students.txt               (Auto-generated when program runs)
```

You can also split into packages if needed (`model/`, `service/`, `util/`).

---

## 🖥️ How to Run

### **1. Compile the program**

```
javac StudentRecordSystem.java
```

### **2. Run the program**

```
java StudentRecordSystem
```

### **3. Output File Generated**

The file `students.txt` will be created automatically to store all student data.

---

## 📑 Menu Options

When you run the program, you will see:

```
===== Capstone Student Menu =====
1. Add Student
2. View All Students
3. Search by Name
4. Delete by Name
5. Sort by Marks
6. Update by RollNo
7. Save and Exit
```

---

## 📌 Technologies Used

* **Java SE 8+**
* **Collections Framework** (List, Map, Iterator)
* **OOP Concepts**
* **Exception Handling**
* **Multithreading**
* **File I/O**

---

## 📘 Assignment Requirements Covered

### ✔ As per Lab Assignment 5 (file provided)

* Abstract class + inheritance ✔
* Interface implementation ✔
* Exception handling ✔
* Custom exceptions ✔
* File handling with buffered I/O ✔
* Sorting with Comparator ✔
* Display using Iterator ✔
* Multithreading (Loader) ✔

---

## 📄 students.txt Format

Each student is stored as a single line:

```
rollNo|name|email|course|marks|grade
```

Example:

```
101|Rahul|rahul@mail.com|B.Tech|85.0|A
```

---

## 🏁 End of Program

When user selects **Save and Exit**, all data is written to `students.txt` and the program closes.

---

## 🧑‍💻 Author

**Parintia singh** (Student)
2401010252
