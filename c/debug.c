#include <cspublic.h>
#include <ctpublic.h>
#include <assert.h>
#include <stdio.h>

static
CS_RETCODE csmsg_fn(CS_CONTEXT *context, CS_CLIENTMSG *emsgp)
{
    fprintf(stderr,
        "CS-Library error:\n");
    fprintf(stderr,
        "\tseverity(%ld) layer(%ld) origin(%ld) number(%ld)",
        (long)CS_SEVERITY(emsgp->msgnumber),
        (long)CS_LAYER(emsgp->msgnumber),
        (long)CS_ORIGIN(emsgp->msgnumber),
        (long)CS_NUMBER(emsgp->msgnumber));

    fprintf(stderr, "\t%s\n", emsgp->msgstring);

    if (emsgp->osstringlen > 0) {
        fprintf(stderr, "Operating System Error: %s\n",
            emsgp->osstring);
    }

    return (CS_SUCCEED);
}

static
CS_RETCODE clientmsg_fn(CS_CONTEXT *context, CS_CONNECTION *conn, CS_CLIENTMSG *emsgp)
{
    fprintf(stderr,
        "Client Library error:\n\t");
    fprintf(stderr,
        "severity(%ld) number(%ld) origin(%ld) layer(%ld)\n",
        (long)CS_SEVERITY(emsgp->severity),
        (long)CS_NUMBER(emsgp->msgnumber),
        (long)CS_ORIGIN(emsgp->msgnumber),
        (long)CS_LAYER(emsgp->msgnumber));

    fprintf(stderr, "\t%s\n", emsgp->msgstring);

    if (emsgp->osstringlen > 0) {
        fprintf(stderr,
            "Operating system error number(%ld):\n",
            (long)emsgp->osnumber);
        fprintf(stderr, "\t%s\n", emsgp->osstring);
    }
    return (CS_SUCCEED);
}

static
CS_RETCODE servermsg_fn(CS_CONTEXT *cp, CS_CONNECTION *chp, CS_SERVERMSG *msgp)
{
    fprintf(stderr,
        "Server message:\n\t");
    fprintf(stderr,
        "number(%ld) severity(%ld) state(%ld) line(%ld)\n",
        (long)msgp->msgnumber, (long)msgp->severity,
        (long)msgp->state, (long)msgp->line);

    if (msgp->svrnlen > 0)
        fprintf(stderr, "\tServer name: %s\n", msgp->svrname);
    if (msgp->proclen > 0)
        fprintf(stderr, "\tProcedure name: %s\n", msgp->proc);

    fprintf(stderr, "\t%s\n", msgp->text);
    return (CS_SUCCEED);
}

int debug1(CS_CONTEXT* ctx)
{
    CS_INT ret = cs_config(
        ctx,
        CS_SET,
        CS_MESSAGE_CB,
        (CS_VOID*)csmsg_fn,
        CS_UNUSED,
        NULL);
    assert(ret == CS_SUCCEED);

    ret = ct_callback(
        ctx,
        NULL,
        CS_SET,
        CS_CLIENTMSG_CB,
        (CS_VOID*)clientmsg_fn);
    assert(ret == CS_SUCCEED);

    ret = ct_callback(
        ctx,
        NULL,
        CS_SET,
        CS_SERVERMSG_CB,
        (CS_VOID*)servermsg_fn);
    assert(ret == CS_SUCCEED);

    return CS_SUCCEED;
}

