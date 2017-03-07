from querygraph.language.compiler import QGLCompiler


def read_qgl(file_path):
    with open(file_path, 'r') as qgl_file:
        query_str = qgl_file.read()
    compiler = QGLCompiler(qgl_str=query_str)
    return compiler.compile()


