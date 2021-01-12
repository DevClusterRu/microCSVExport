package Utils

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func Pop(s []string, e string) []string { //Popping user from user stack
	for k, a := range s {
		if a == e {
			return Remove(s,k)
		}
	}
	return s
}

////////////////////////////////////////////////////////

func Remove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

