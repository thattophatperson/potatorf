/*
 * potatorf.c — Lightweight file-based database manager in C
 *
 * Commands: CREATE TABLE, INSERT INTO, SELECT, UPDATE, DELETE FROM,
 *           DROP TABLE, SHOW TABLES, DESCRIBE, VACUUM
 *
 * Build:  gcc -Wall -O2 -o potatorf potatorf.c
 * Usage:  ./potatorf <db.dbm>            — interactive REPL
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <time.h>

/* strcasestr / strncasecmp are GNU/POSIX extensions not available on Windows.
   Define portable replacements when building with MinGW / MSVC. */
#if defined(_WIN32)
#  define strncasecmp _strnicmp
#  define strcasecmp  _stricmp
static char *my_strcasestr(const char *hay, const char *needle) {
    if (!*needle) return (char *)hay;
    size_t nlen = strlen(needle);
    for (; *hay; hay++)
        if (_strnicmp(hay, needle, nlen) == 0) return (char *)hay;
    return NULL;
}
#  define strcasestr my_strcasestr
#endif

/* ── Constants ─────────────────────────────────────────────── */
#define MAX_TABLES   64
#define MAX_COLUMNS  32
#define MAX_NAME_LEN 64
#define MAX_STR_LEN  256
#define MAX_SQL_LEN  4096
#define DB_MAGIC     0x444D4742u

/* ── Types ──────────────────────────────────────────────────── */
typedef enum { T_INT=1, T_FLOAT=2, T_TEXT=3, T_BOOL=4 } CType;

typedef union { int64_t i; double f; char s[MAX_STR_LEN]; int8_t b; } Val;

typedef struct { char name[MAX_NAME_LEN]; CType type; int8_t nullable, pk; } Col;

typedef struct { Val data[MAX_COLUMNS]; int8_t null[MAX_COLUMNS], del; } Row;

typedef struct {
    char  name[MAX_NAME_LEN];
    int   ncols, nrows, cap, next_id;
    Col   cols[MAX_COLUMNS];
    Row  *rows;
} Table;

typedef struct {
    uint32_t magic, version;
    int      ntables;
    char     name[MAX_NAME_LEN], created[32];
} DBHdr;

typedef struct { DBHdr hdr; Table tbl[MAX_TABLES]; char file[512]; } DB;

/* Dynamic result set */
typedef struct {
    int   ok, nrows, ncols, cap, affected;
    char  msg[1024];
    char  cname[MAX_COLUMNS][MAX_NAME_LEN];
    CType ctype[MAX_COLUMNS];
    char **cells;   /* flat: row*ncols+col */
} Res;

/* ── Result helpers ─────────────────────────────────────────── */
static void res_ok(Res *r, const char *m, int n)
    { r->ok=1; snprintf(r->msg,sizeof(r->msg),"%s",m); r->affected=n; }
static void res_err(Res *r, const char *m)
    { r->ok=0; snprintf(r->msg,sizeof(r->msg),"%s",m); }

static void res_addrow(Res *r, char v[][MAX_STR_LEN], int nc){
    if(r->nrows>=r->cap){
        int nc2=r->cap?r->cap*2:64;
        r->cells=(char**)realloc(r->cells,sizeof(char*)*nc2*nc);
        memset(r->cells+r->cap*nc,0,sizeof(char*)*(nc2-r->cap)*nc);
        r->cap=nc2;
    }
    for(int j=0;j<nc;j++){
        char **c=&r->cells[r->nrows*nc+j];
        free(*c); *c=strdup(v[j]);
    }
    r->nrows++;
}
static const char *res_get(Res *r,int row,int col){
    if(!r->cells) return "";
    char *v=r->cells[row*r->ncols+col]; return v?v:"";
}
static void res_free(Res *r){
    if(r&&r->cells){
        for(int i=0;i<r->cap*r->ncols;i++) free(r->cells[i]);
        free(r->cells);
    }
    free(r);
}

/* ── Utility ────────────────────────────────────────────────── */
static void strtrim(char *s){
    char *a=s; while(isspace((unsigned char)*a))a++;
    memmove(s,a,strlen(a)+1);
    char *e=s+strlen(s); while(e>s&&isspace((unsigned char)*(e-1)))*--e='\0';
}
static int strswci(const char *s,const char *p){ return strncasecmp(s,p,strlen(p))==0; }

static const char *tname(CType t){
    switch(t){case T_INT:return"INT";case T_FLOAT:return"FLOAT";
              case T_TEXT:return"TEXT";case T_BOOL:return"BOOL";default:return"?";}
}
static CType tparse(const char *s){
    if(!strcasecmp(s,"INT")||!strcasecmp(s,"INTEGER"))return T_INT;
    if(!strcasecmp(s,"FLOAT")||!strcasecmp(s,"DOUBLE")||!strcasecmp(s,"REAL"))return T_FLOAT;
    if(!strcasecmp(s,"TEXT")||!strcasecmp(s,"VARCHAR")||!strcasecmp(s,"STRING"))return T_TEXT;
    if(!strcasecmp(s,"BOOL")||!strcasecmp(s,"BOOLEAN"))return T_BOOL;
    return 0;
}
static void val2str(Val *v,CType t,char *o,size_t n){
    switch(t){case T_INT:snprintf(o,n,"%lld",(long long)v->i);break;
              case T_FLOAT:snprintf(o,n,"%.6g",v->f);break;
              case T_TEXT:snprintf(o,n,"%s",v->s);break;
              case T_BOOL:snprintf(o,n,"%s",v->b?"true":"false");break;
              default:snprintf(o,n,"NULL");}
}
static void str2val(const char *s,CType t,Val *v){
    switch(t){case T_INT:v->i=strtoll(s,NULL,10);break;
              case T_FLOAT:v->f=strtod(s,NULL);break;
              case T_TEXT:strncpy(v->s,s,MAX_STR_LEN-1);v->s[MAX_STR_LEN-1]=0;break;
              case T_BOOL:v->b=(!strcasecmp(s,"true")||!strcmp(s,"1"))?1:0;break;
              default:break;}
}

/* ── DB I/O ─────────────────────────────────────────────────── */
static Table *find_tbl(DB *db,const char *n){
    for(int i=0;i<db->hdr.ntables;i++)
        if(!strcasecmp(db->tbl[i].name,n)) return &db->tbl[i];
    return NULL;
}
static int save_db(DB *db){
    FILE *f=fopen(db->file,"wb"); if(!f) return -1;
    fwrite(&db->hdr,sizeof(DBHdr),1,f);
    for(int i=0;i<db->hdr.ntables;i++){
        Table *t=&db->tbl[i];
        fwrite(t->name,MAX_NAME_LEN,1,f);
        fwrite(&t->ncols,sizeof(int),1,f);
        fwrite(t->cols,sizeof(Col)*t->ncols,1,f);
        fwrite(&t->nrows,sizeof(int),1,f);
        fwrite(&t->next_id,sizeof(int),1,f);
        for(int j=0;j<t->nrows;j++) fwrite(&t->rows[j],sizeof(Row),1,f);
    }
    fclose(f); return 0;
}
static int load_db(DB *db){
    FILE *f=fopen(db->file,"rb"); if(!f) return -1;
    if(fread(&db->hdr,sizeof(DBHdr),1,f)!=1){fclose(f);return -1;}
    if(db->hdr.magic!=DB_MAGIC){fclose(f);return -2;}
    for(int i=0;i<db->hdr.ntables;i++){
        Table *t=&db->tbl[i];
        if(!fread(t->name,MAX_NAME_LEN,1,f)) break;
        if(!fread(&t->ncols,sizeof(int),1,f)) break;
        if(!fread(t->cols,sizeof(Col)*t->ncols,1,f)) break;
        if(!fread(&t->nrows,sizeof(int),1,f)) break;
        if(!fread(&t->next_id,sizeof(int),1,f)) break;
        t->cap=t->nrows>0?t->nrows*2:16;
        t->rows=(Row*)malloc(sizeof(Row)*t->cap);
        if(!t->rows){fclose(f);return -3;}
        for(int j=0;j<t->nrows;j++)
            if(!fread(&t->rows[j],sizeof(Row),1,f)) break;
    }
    fclose(f); return 0;
}
static DB *open_db(const char *fn){
    DB *db=(DB*)calloc(1,sizeof(DB)); if(!db) return NULL;
    strncpy(db->file,fn,sizeof(db->file)-1);
    if(load_db(db)==0) return db;
    db->hdr.magic=DB_MAGIC; db->hdr.version=1;
    const char *b=strrchr(fn,'/'); b=b?b+1:fn;
    strncpy(db->hdr.name,b,MAX_NAME_LEN-1);
    char *d=strrchr(db->hdr.name,'.'); if(d)*d=0;
    time_t now=time(NULL); struct tm *tm=localtime(&now);
    strftime(db->hdr.created,32,"%Y-%m-%d %H:%M:%S",tm);
    return db;
}
static void close_db(DB *db){
    if(!db) return;
    save_db(db);
    for(int i=0;i<db->hdr.ntables;i++) free(db->tbl[i].rows);
    free(db);
}

/* ── WHERE ──────────────────────────────────────────────────── */
typedef struct { char col[MAX_NAME_LEN],op[4],val[MAX_STR_LEN]; int isnull,nullexp; } Cond;

static int parse_cond(const char *w,Cond *c){
    char tmp[MAX_SQL_LEN]; strncpy(tmp,w,sizeof(tmp)-1); strtrim(tmp);
    char *p;
    if((p=strcasestr(tmp," IS NOT NULL"))){*p=0;strtrim(tmp);strncpy(c->col,tmp,MAX_NAME_LEN-1);c->isnull=1;c->nullexp=0;return 1;}
    if((p=strcasestr(tmp," IS NULL"))){*p=0;strtrim(tmp);strncpy(c->col,tmp,MAX_NAME_LEN-1);c->isnull=1;c->nullexp=1;return 1;}
    c->isnull=0;
    const char *ops[]={"<=",">=","!=","<>","=","<",">",NULL};
    for(int i=0;ops[i];i++){
        if((p=strstr(tmp,ops[i]))){
            *p=0; strncpy(c->col,tmp,MAX_NAME_LEN-1); strtrim(c->col);
            strncpy(c->op,strcmp(ops[i],"<>")?"!=":ops[i],3); /* use != for <> */
            strncpy(c->op,ops[i],3); if(!strcmp(c->op,"<>")) strcpy(c->op,"!=");
            char *v=p+strlen(ops[i]); strtrim(v);
            if(*v=='\''||*v=='"'){v++;char *e=v+strlen(v)-1;if(*e=='\''||*e=='"')*e=0;}
            strncpy(c->val,v,MAX_STR_LEN-1); return 1;
        }
    }
    return 0;
}
static int eval_cond(Row *row,Table *t,Cond *c){
    int ci=-1;
    for(int i=0;i<t->ncols;i++) if(!strcasecmp(t->cols[i].name,c->col)){ci=i;break;}
    if(ci<0) return 0;
    if(c->isnull) return c->nullexp?row->null[ci]:!row->null[ci];
    if(row->null[ci]) return 0;
    Val *v=&row->data[ci]; CType tp=t->cols[ci].type;
    Val cv; str2val(c->val,tp,&cv);
    int cmp=0;
    if(tp==T_INT)  cmp=(v->i>cv.i)-(v->i<cv.i);
    else if(tp==T_FLOAT) cmp=(v->f>cv.f)-(v->f<cv.f);
    else if(tp==T_TEXT)  cmp=strcasecmp(v->s,cv.s);
    else if(tp==T_BOOL)  cmp=v->b-cv.b;
    if(!strcmp(c->op,"="))  return cmp==0;
    if(!strcmp(c->op,"!=")) return cmp!=0;
    if(!strcmp(c->op,"<"))  return cmp<0;
    if(!strcmp(c->op,">"))  return cmp>0;
    if(!strcmp(c->op,"<=")) return cmp<=0;
    if(!strcmp(c->op,">=")) return cmp>=0;
    return 0;
}

/* ── Commands ───────────────────────────────────────────────── */
static void do_create(DB *db,char *sql,Res *r){
    if(db->hdr.ntables>=MAX_TABLES){res_err(r,"Max tables reached");return;}
    char *p=sql+12; while(isspace((unsigned char)*p))p++;
    char tn[MAX_NAME_LEN]={0}; int i=0;
    while(*p&&!isspace((unsigned char)*p)&&*p!='('&&i<MAX_NAME_LEN-1) tn[i++]=*p++;
    while(isspace((unsigned char)*p))p++;
    if(*p!='('){res_err(r,"Expected '('");return;}
    if(find_tbl(db,tn)){char m[128];snprintf(m,128,"Table '%s' exists",tn);res_err(r,m);return;}
    p++; char *end=strrchr(p,')'); if(!end){res_err(r,"Missing ')'");return;} *end=0;
    Table *t=&db->tbl[db->hdr.ntables];
    memset(t,0,sizeof(Table));
    strncpy(t->name,tn,MAX_NAME_LEN-1);
    t->cap=16; t->rows=(Row*)malloc(sizeof(Row)*t->cap);
    if(!t->rows){res_err(r,"OOM");return;}
    char buf[MAX_SQL_LEN]; strncpy(buf,p,sizeof(buf)-1);
    char *cd=strtok(buf,",");
    while(cd&&t->ncols<MAX_COLUMNS){
        strtrim(cd); if(!*cd){cd=strtok(NULL,",");continue;}
        int ispk=strcasestr(cd,"PRIMARY KEY")?1:0;
        char cn[MAX_NAME_LEN]={0},cs[32]={0};
        sscanf(cd,"%63s %31s",cn,cs);
        CType ct=tparse(cs);
        if(!ct){char m[128];snprintf(m,128,"Unknown type '%s'",cs);res_err(r,m);free(t->rows);return;}
        Col *col=&t->cols[t->ncols];
        strncpy(col->name,cn,MAX_NAME_LEN-1); col->type=ct; col->pk=ispk;
        col->nullable=strcasestr(cd,"NOT NULL")?0:1; t->ncols++;
        cd=strtok(NULL,",");
    }
    if(!t->ncols){res_err(r,"No columns defined");free(t->rows);return;}
    db->hdr.ntables++;
    save_db(db);
    char m[128];snprintf(m,128,"Table '%s' created (%d cols)",tn,t->ncols);res_ok(r,m,0);
}

static void do_drop(DB *db,char *sql,Res *r){
    char *p=sql+10; strtrim(p);
    Table *t=find_tbl(db,p);
    if(!t){char m[128];snprintf(m,128,"Table '%s' not found",p);res_err(r,m);return;}
    int idx=(int)(t-db->tbl);
    free(t->rows);
    for(int i=idx;i<db->hdr.ntables-1;i++) db->tbl[i]=db->tbl[i+1];
    db->hdr.ntables--;
    save_db(db);
    char m[128];snprintf(m,128,"Table '%s' dropped",p);res_ok(r,m,0);
}

static void do_insert(DB *db,char *sql,Res *r){
    char *p=sql+11; while(isspace((unsigned char)*p))p++;
    char tn[MAX_NAME_LEN]={0}; int i=0;
    while(*p&&!isspace((unsigned char)*p)&&*p!='('&&i<MAX_NAME_LEN-1) tn[i++]=*p++;
    while(isspace((unsigned char)*p))p++;
    Table *t=find_tbl(db,tn);
    if(!t){char m[128];snprintf(m,128,"Table '%s' not found",tn);res_err(r,m);return;}
    int ord[MAX_COLUMNS],ns=0;
    if(*p=='('){
        p++; char *e=strchr(p,')'); if(!e){res_err(r,"Missing ')'");return;} *e=0;
        char buf[MAX_SQL_LEN]; strncpy(buf,p,sizeof(buf)-1);
        char *cn=strtok(buf,",");
        while(cn&&ns<MAX_COLUMNS){
            strtrim(cn); int f=-1;
            for(int j=0;j<t->ncols;j++) if(!strcasecmp(t->cols[j].name,cn)){f=j;break;}
            if(f<0){char m[128];snprintf(m,128,"Column '%s' not found",cn);res_err(r,m);return;}
            ord[ns++]=f; cn=strtok(NULL,",");
        }
        p=e+1; while(isspace((unsigned char)*p))p++;
    } else { for(int j=0;j<t->ncols;j++) ord[j]=j; ns=t->ncols; }
    char *vs=strcasestr(p,"VALUES"); if(!vs){res_err(r,"Missing VALUES");return;}
    vs+=6; while(isspace((unsigned char)*vs))vs++;
    if(*vs!='('){res_err(r,"Expected '('");return;} vs++;
    char *ve=strrchr(vs,')'); if(!ve){res_err(r,"Missing ')'");return;} *ve=0;
    if(t->nrows>=t->cap){
        t->cap*=2; t->rows=(Row*)realloc(t->rows,sizeof(Row)*t->cap);
        if(!t->rows){res_err(r,"OOM");return;}
    }
    Row *row=&t->rows[t->nrows];
    memset(row,0,sizeof(Row));
    for(int j=0;j<t->ncols;j++) row->null[j]=1;
    int vi=0; char *vp=vs;
    while(*vp&&vi<ns){
        while(isspace((unsigned char)*vp))vp++;
        char vb[MAX_STR_LEN]={0}; int bi=0;
        if(*vp=='\''||*vp=='"'){
            char q=*vp++;
            while(*vp&&*vp!=q&&bi<MAX_STR_LEN-1) vb[bi++]=*vp++;
            if(*vp==q) vp++;
        } else {
            while(*vp&&*vp!=','&&bi<MAX_STR_LEN-1) vb[bi++]=*vp++;
            strtrim(vb);
        }
        while(*vp==','||isspace((unsigned char)*vp)) vp++;
        int ci=ord[vi];
        if(!strcasecmp(vb,"NULL")) row->null[ci]=1;
        else{row->null[ci]=0;str2val(vb,t->cols[ci].type,&row->data[ci]);}
        vi++;
    }
    t->nrows++; t->next_id++;
    save_db(db);
    res_ok(r,"1 row inserted",1);
}

static void do_select(DB *db,char *sql,Res *r){
    char *p=sql+6; while(isspace((unsigned char)*p))p++;
    char *from=strcasestr(p,"FROM"); if(!from){res_err(r,"Missing FROM");return;}
    char cl[MAX_SQL_LEN]={0}; strncpy(cl,p,(size_t)(from-p)); strtrim(cl);
    p=from+4; while(isspace((unsigned char)*p))p++;
    char tn[MAX_NAME_LEN]={0}; int i=0;
    while(*p&&!isspace((unsigned char)*p)&&i<MAX_NAME_LEN-1) tn[i++]=*p++;
    while(isspace((unsigned char)*p))p++;
    Table *t=find_tbl(db,tn);
    if(!t){char m[128];snprintf(m,128,"Table '%s' not found",tn);res_err(r,m);return;}
    Cond c; int hc=0;
    char *wh=strcasestr(p,"WHERE");
    if(wh){wh+=5;strtrim(wh);hc=parse_cond(wh,&c);}
    int oc[MAX_COLUMNS],no=0;
    if(!strcmp(cl,"*")){for(int j=0;j<t->ncols;j++) oc[no++]=j;}
    else{
        char buf[MAX_SQL_LEN]; strncpy(buf,cl,sizeof(buf)-1);
        char *cn=strtok(buf,",");
        while(cn&&no<MAX_COLUMNS){
            strtrim(cn); int f=-1;
            for(int j=0;j<t->ncols;j++) if(!strcasecmp(t->cols[j].name,cn)){f=j;break;}
            if(f<0){char m[128];snprintf(m,128,"Column '%s' not found",cn);res_err(r,m);return;}
            oc[no++]=f; cn=strtok(NULL,",");
        }
    }
    r->ok=1; r->ncols=no;
    for(int j=0;j<no;j++){strncpy(r->cname[j],t->cols[oc[j]].name,MAX_NAME_LEN-1);r->ctype[j]=t->cols[oc[j]].type;}
    char rv[MAX_COLUMNS][MAX_STR_LEN];
    for(int j=0;j<t->nrows;j++){
        Row *row=&t->rows[j]; if(row->del) continue;
        if(hc&&!eval_cond(row,t,&c)) continue;
        for(int k=0;k<no;k++){
            int ci=oc[k];
            if(row->null[ci]) strcpy(rv[k],"NULL");
            else val2str(&row->data[ci],t->cols[ci].type,rv[k],MAX_STR_LEN);
        }
        res_addrow(r,rv,no);
    }
    char m[64];snprintf(m,64,"%d row(s) returned",r->nrows);
    strncpy(r->msg,m,sizeof(r->msg)-1); r->affected=r->nrows;
}

static void do_update(DB *db,char *sql,Res *r){
    char *p=sql+6; while(isspace((unsigned char)*p))p++;
    char tn[MAX_NAME_LEN]={0}; int i=0;
    while(*p&&!isspace((unsigned char)*p)&&i<MAX_NAME_LEN-1) tn[i++]=*p++;
    while(isspace((unsigned char)*p))p++;
    Table *t=find_tbl(db,tn);
    if(!t){char m[128];snprintf(m,128,"Table '%s' not found",tn);res_err(r,m);return;}
    if(!strswci(p,"SET")){res_err(r,"Expected SET");return;}
    p+=3; while(isspace((unsigned char)*p))p++;
    char *wkw=strcasestr(p,"WHERE");
    Cond c; int hc=0;
    char sc[MAX_SQL_LEN]={0};
    if(wkw){strncpy(sc,p,(size_t)(wkw-p));strtrim(sc);char *wh=wkw+5;strtrim(wh);hc=parse_cond(wh,&c);}
    else{strncpy(sc,p,sizeof(sc)-1);strtrim(sc);}
    char scols[MAX_COLUMNS][MAX_NAME_LEN], svals[MAX_COLUMNS][MAX_STR_LEN]; int ns=0;
    char *a=strtok(sc,",");
    while(a&&ns<MAX_COLUMNS){
        strtrim(a);
        char *eq=strchr(a,'='); if(!eq){res_err(r,"Bad SET");return;}
        *eq=0; strncpy(scols[ns],a,MAX_NAME_LEN-1); strtrim(scols[ns]);
        char *sv=eq+1; strtrim(sv);
        if(*sv=='\''||*sv=='"'){sv++;char *e=sv+strlen(sv)-1;if(*e=='\''||*e=='"')*e=0;}
        strncpy(svals[ns],sv,MAX_STR_LEN-1); ns++;
        a=strtok(NULL,",");
    }
    int upd=0;
    for(int j=0;j<t->nrows;j++){
        Row *row=&t->rows[j]; if(row->del) continue;
        if(hc&&!eval_cond(row,t,&c)) continue;
        for(int k=0;k<ns;k++){
            int ci=-1;
            for(int m=0;m<t->ncols;m++) if(!strcasecmp(t->cols[m].name,scols[k])){ci=m;break;}
            if(ci<0) continue;
            if(!strcasecmp(svals[k],"NULL")) row->null[ci]=1;
            else{row->null[ci]=0;str2val(svals[k],t->cols[ci].type,&row->data[ci]);}
        }
        upd++;
    }
    save_db(db);
    char m[64];snprintf(m,64,"%d row(s) updated",upd);res_ok(r,m,upd);
}

static void do_delete(DB *db,char *sql,Res *r){
    char *p=sql+11; while(isspace((unsigned char)*p))p++;
    char tn[MAX_NAME_LEN]={0}; int i=0;
    while(*p&&!isspace((unsigned char)*p)&&i<MAX_NAME_LEN-1) tn[i++]=*p++;
    while(isspace((unsigned char)*p))p++;
    Table *t=find_tbl(db,tn);
    if(!t){char m[128];snprintf(m,128,"Table '%s' not found",tn);res_err(r,m);return;}
    Cond c; int hc=0;
    char *wh=strcasestr(p,"WHERE");
    if(wh){wh+=5;strtrim(wh);hc=parse_cond(wh,&c);}
    int del=0;
    for(int j=0;j<t->nrows;j++){
        Row *row=&t->rows[j]; if(row->del) continue;
        if(hc&&!eval_cond(row,t,&c)) continue;
        row->del=1; del++;
    }
    save_db(db);
    char m[64];snprintf(m,64,"%d row(s) deleted",del);res_ok(r,m,del);
}

static void do_show(DB *db,Res *r){
    r->ok=1; r->ncols=3;
    strcpy(r->cname[0],"Table");   r->ctype[0]=T_TEXT;
    strcpy(r->cname[1],"Columns"); r->ctype[1]=T_INT;
    strcpy(r->cname[2],"Rows");    r->ctype[2]=T_INT;
    char v[MAX_COLUMNS][MAX_STR_LEN];
    for(int i=0;i<db->hdr.ntables;i++){
        Table *t=&db->tbl[i]; int rc=0;
        for(int j=0;j<t->nrows;j++) if(!t->rows[j].del) rc++;
        strncpy(v[0],t->name,MAX_STR_LEN-1);
        snprintf(v[1],MAX_STR_LEN,"%d",t->ncols);
        snprintf(v[2],MAX_STR_LEN,"%d",rc);
        res_addrow(r,v,3);
    }
    char m[64];snprintf(m,64,"%d table(s)",r->nrows);
    strncpy(r->msg,m,sizeof(r->msg)-1); r->affected=r->nrows;
}

static void do_desc(DB *db,char *sql,Res *r){
    char *p=sql; while(*p&&!isspace((unsigned char)*p))p++; strtrim(p);
    Table *t=find_tbl(db,p);
    if(!t){char m[128];snprintf(m,128,"Table '%s' not found",p);res_err(r,m);return;}
    r->ok=1; r->ncols=4;
    strcpy(r->cname[0],"Column");  r->ctype[0]=T_TEXT;
    strcpy(r->cname[1],"Type");    r->ctype[1]=T_TEXT;
    strcpy(r->cname[2],"Nullable");r->ctype[2]=T_TEXT;
    strcpy(r->cname[3],"PK");      r->ctype[3]=T_TEXT;
    char v[MAX_COLUMNS][MAX_STR_LEN];
    for(int i=0;i<t->ncols;i++){
        strncpy(v[0],t->cols[i].name,MAX_STR_LEN-1);
        strncpy(v[1],tname(t->cols[i].type),MAX_STR_LEN-1);
        strcpy(v[2],t->cols[i].nullable?"YES":"NO");
        strcpy(v[3],t->cols[i].pk?"YES":"NO");
        res_addrow(r,v,4);
    }
    char m[128];snprintf(m,128,"Table '%s': %d column(s)",t->name,t->ncols);
    strncpy(r->msg,m,sizeof(r->msg)-1);
}

static void do_vacuum(DB *db,Res *r){
    int tot=0;
    for(int i=0;i<db->hdr.ntables;i++){
        Table *t=&db->tbl[i]; int w=0;
        for(int j=0;j<t->nrows;j++)
            if(!t->rows[j].del) t->rows[w++]=t->rows[j]; else tot++;
        t->nrows=w;
    }
    save_db(db);
    char m[64];snprintf(m,64,"VACUUM: purged %d row(s)",tot);res_ok(r,m,tot);
}

/* ── Dispatcher ─────────────────────────────────────────────── */
void db_exec(DB *db,const char *in,Res *r){
    memset(r,0,sizeof(*r));
    char sql[MAX_SQL_LEN]; strncpy(sql,in,MAX_SQL_LEN-1); strtrim(sql);
    int l=(int)strlen(sql); if(l>0&&sql[l-1]==';') sql[--l]=0; strtrim(sql);
    if(!*sql){res_ok(r,"Empty",0);return;}
    if(strswci(sql,"CREATE TABLE"))     do_create(db,sql,r);
    else if(strswci(sql,"DROP TABLE"))  do_drop(db,sql,r);
    else if(strswci(sql,"INSERT INTO")) do_insert(db,sql,r);
    else if(strswci(sql,"SELECT"))      do_select(db,sql,r);
    else if(strswci(sql,"UPDATE"))      do_update(db,sql,r);
    else if(strswci(sql,"DELETE FROM")) do_delete(db,sql,r);
    else if(strswci(sql,"SHOW TABLES")) do_show(db,r);
    else if(strswci(sql,"DESCRIBE")||strswci(sql,"DESC ")) do_desc(db,sql,r);
    else if(strswci(sql,"VACUUM"))      do_vacuum(db,r);
    else res_err(r,"Unknown command");
}

/* ── Printer ────────────────────────────────────────────────── */
static void print_res(Res *r){
    if(!r->ok){fprintf(stderr,"ERROR: %s\n",r->msg);return;}
    if(!r->ncols){printf("OK: %s\n",r->msg);return;}
    int w[MAX_COLUMNS];
    for(int j=0;j<r->ncols;j++){
        w[j]=(int)strlen(r->cname[j]);
        for(int i=0;i<r->nrows;i++){int l=(int)strlen(res_get(r,i,j));if(l>w[j])w[j]=l;}
    }
    printf("+");for(int j=0;j<r->ncols;j++){for(int k=0;k<w[j]+2;k++)printf("-");printf("+");}printf("\n");
    printf("|");for(int j=0;j<r->ncols;j++) printf(" %-*s |",w[j],r->cname[j]);printf("\n");
    printf("+");for(int j=0;j<r->ncols;j++){for(int k=0;k<w[j]+2;k++)printf("-");printf("+");}printf("\n");
    for(int i=0;i<r->nrows;i++){
        printf("|");for(int j=0;j<r->ncols;j++) printf(" %-*s |",w[j],res_get(r,i,j));printf("\n");
    }
    printf("+");for(int j=0;j<r->ncols;j++){for(int k=0;k<w[j]+2;k++)printf("-");printf("+");}printf("\n");
    printf("%s\n",r->msg);
}

/* ── Main ───────────────────────────────────────────────────── */
int main(int argc,char *argv[]){
    if(argc<2){
        fprintf(stderr,"Usage:\n  %s <db.dbm>         — REPL\n  %s <db.dbm> \"SQL\"  — single command\n",argv[0],argv[0]);
        return 1;
    }
    char fn[512]; strncpy(fn,argv[1],sizeof(fn)-1);
    if(!strstr(fn,".dbm")) strncat(fn,".dbm",sizeof(fn)-strlen(fn)-1);
    DB *db=open_db(fn);
    if(!db){fprintf(stderr,"Fatal: cannot open '%s'\n",fn);return 1;}
    printf("potatorf v1.0  db=%s  tables=%d\n",db->hdr.name,db->hdr.ntables);
    Res *r=(Res*)calloc(1,sizeof(Res)); if(!r){close_db(db);return 1;}
    if(argc>=3){
        char sql[MAX_SQL_LEN]={0};
        for(int i=2;i<argc;i++){strncat(sql,argv[i],sizeof(sql)-strlen(sql)-1);if(i<argc-1)strncat(sql," ",sizeof(sql)-strlen(sql)-1);}
        db_exec(db,sql,r); print_res(r); res_free(r);
    } else {
        printf("Type SQL (end with ;) or 'quit'.\n\n");
        char line[MAX_SQL_LEN],buf[MAX_SQL_LEN]={0};
        while(1){
            printf(*buf==0?"db> ":"... "); fflush(stdout);
            if(!fgets(line,sizeof(line),stdin)) break;
            strtrim(line);
            if(!strcasecmp(line,"quit")||!strcasecmp(line,"exit")) break;
            if(!*line) continue;
            strncat(buf,line,sizeof(buf)-strlen(buf)-1);
            strncat(buf," ",sizeof(buf)-strlen(buf)-1);
            if(strchr(line,';')||strswci(buf,"SHOW")||strswci(buf,"VACUUM")||strswci(buf,"DESC")){
                if(r->cells){for(int i=0;i<r->cap*r->ncols;i++) free(r->cells[i]);free(r->cells);}
                memset(r,0,sizeof(*r));
                db_exec(db,buf,r); print_res(r); buf[0]=0;
            }
        }
        if(r->cells){for(int i=0;i<r->cap*r->ncols;i++) free(r->cells[i]);free(r->cells);}
        free(r);
    }
    close_db(db); printf("Goodbye.\n"); return 0;
}
