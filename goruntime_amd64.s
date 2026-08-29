#include "textflag.h"

TEXT ·getg(SB), NOSPLIT, $0-8
	MOVQ R14, ret+0(FP)
	RET
