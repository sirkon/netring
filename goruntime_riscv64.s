#include "textflag.h"

TEXT ·getg(SB), NOSPLIT, $0-8
	MOV g, ret+0(FP)
	RET
