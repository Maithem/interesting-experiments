mpz_aorsmul (mpz_ptr w, mpz_srcptr x, mpz_srcptr y, mp_size_t sub)
{
  mp_size_t  xsize, ysize, tsize, wsize, wsize_signed;
  mp_ptr     wp, tp;
  mp_limb_t  c, high;
  TMP_DECL;

  /* w unaffected if x==0 or y==0 */
  xsize = SIZ(x);
  ysize = SIZ(y);
  if (xsize == 0 || ysize == 0)
    return;

  /* make x the bigger of the two */
  if (ABS(ysize) > ABS(xsize))
    {
      MPZ_SRCPTR_SWAP (x, y);
      MP_SIZE_T_SWAP (xsize, ysize);
    }

  sub ^= ysize;
  ysize = ABS(ysize);

  /* use mpn_addmul_1/mpn_submul_1 if possible */
  if (ysize == 1)
    {
      mpz_aorsmul_1 (w, x, PTR(y)[0], sub);
      return;
    }

  sub ^= xsize;
  xsize = ABS(xsize);

  wsize_signed = SIZ(w);
  sub ^= wsize_signed;
  wsize = ABS(wsize_signed);

  tsize = xsize + ysize;
  wp = MPZ_REALLOC (w, MAX (wsize, tsize) + 1);

  if (wsize_signed == 0)
    {
      /* Nothing to add to, just set w=x*y.  No w==x or w==y overlap here,
	 since we know x,y!=0 but w==0.  */
      high = mpn_mul (wp, PTR(x),xsize, PTR(y),ysize);
      tsize -= (high == 0);
      SIZ(w) = (sub >= 0 ? tsize : -tsize);
      return;
    }

  TMP_MARK;
  tp = TMP_ALLOC_LIMBS (tsize);

  high = mpn_mul (tp, PTR(x),xsize, PTR(y),ysize);
  tsize -= (high == 0);
  ASSERT (tp[tsize-1] != 0);
  if (sub >= 0)
    {
      mp_srcptr up    = wp;
      mp_size_t usize = wsize;

      if (usize < tsize)
	{
	  up	= tp;
	  usize = tsize;
	  tp	= wp;
	  tsize = wsize;

	  wsize = usize;
	}

      c = mpn_add (wp, up,usize, tp,tsize);
      wp[wsize] = c;
      wsize += (c != 0);
    }
  else
    {
      mp_srcptr up    = wp;
      mp_size_t usize = wsize;

      if (mpn_cmp_twosizes_lt (up,usize, tp,tsize))
	{
	  up	= tp;
	  usize = tsize;mpz_aorsmul (mpz_ptr w, mpz_srcptr x, mpz_srcptr y, mp_size_t sub)
{
  mp_size_t  xsize, ysize, tsize, wsize, wsize_signed;
  mp_ptr     wp, tp;
  mp_limb_t  c, high;
  TMP_DECL;

  /* w unaffected if x==0 or y==0 */
  xsize = SIZ(x);
  ysize = SIZ(y);
  if (xsize == 0 || ysize == 0)
    return;

  /* make x the bigger of the two */
  if (ABS(ysize) > ABS(xsize))
    {
      MPZ_SRCPTR_SWAP (x, y);
      MP_SIZE_T_SWAP (xsize, ysize);
    }

  sub ^= ysize;
  ysize = ABS(ysize);

  /* use mpn_addmul_1/mpn_submul_1 if possible */
  if (ysize == 1)
    {
      mpz_aorsmul_1 (w, x, PTR(y)[0], sub);
      return;
    }

  sub ^= xsize;
  xsize = ABS(xsize);

  wsize_signed = SIZ(w);
  sub ^= wsize_signed;
  wsize = ABS(wsize_signed);

  tsize = xsize + ysize;
  wp = MPZ_REALLOC (w, MAX (wsize, tsize) + 1);

  if (wsize_signed == 0)
    {
      /* Nothing to add to, just set w=x*y.  No w==x or w==y overlap here,
	 since we know x,y!=0 but w==0.  */
      high = mpn_mul (wp, PTR(x),xsize, PTR(y),ysize);
      tsize -= (high == 0);
      SIZ(w) = (sub >= 0 ? tsize : -tsize);
      return;
    }

  TMP_MARK;
  tp = TMP_ALLOC_LIMBS (tsize);

  high = mpn_mul (tp, PTR(x),xsize, PTR(y),ysize);
  tsize -= (high == 0);
  ASSERT (tp[tsize-1] != 0);
  if (sub >= 0)
    {
      mp_srcptr up    = wp;
      mp_size_t usize = wsize;

      if (usize < tsize)
	{
	  up	= tp;
	  usize = tsize;
	  tp	= wp;
	  tsize = wsize;

	  wsize = usize;
	}

      c = mpn_add (wp, up,usize, tp,tsize);
      wp[wsize] = c;
      wsize += (c != 0);
    }
  else
    {
	  tp	= wp;
	  tsize = wsize;

	  wsize = usize;
	  wsize_signed = -wsize_signed;
	}

      ASSERT_NOCARRY (mpn_sub (wp, up,usize, tp,tsize));
      wsize = usize;
      MPN_NORMALIZE (wp, wsize);
    }

  SIZ(w) = (wsize_signed >= 0 ? wsize : -wsize);

  TMP_FREE;
}


void
mpz_addmul (mpz_ptr w, mpz_srcptr u, mpz_srcptr v)
{
  mpz_aorsmul (w, u, v, (mp_size_t) 0);
}

void
mpz_submul (mpz_ptr w, mpz_srcptr u, mpz_srcptr v)
{
  mpz_aorsmul (w, u, v, (mp_size_t) -1);
}
