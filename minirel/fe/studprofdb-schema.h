#define MAXSTRLEN	16

#define STUD_FILE	"data.student.sid"
#define STUD_NUM	30
#define STUD_NUM_ATTRS	5
typedef struct student_rec {
	int	sid;
	char	sname[MAXSTRLEN];
	float	gpa;
	int	age;
	int	advisor;
} student;

#define PROF_FILE	"data.professor.pid"
#define PROF_NUM	10
#define PROF_NUM_ATTRS	3
typedef struct faculty_rec {
	int	pid;
	char	pname[MAXSTRLEN];
	int	office;
} professor;
