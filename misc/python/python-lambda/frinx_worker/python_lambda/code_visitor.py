import ast


class FunctionCallVisitor(ast.NodeVisitor):
    """
    NodeVisitor class for analyzing and restricting function calls, imports, and attribute accesses.

    Attributes:
        ALLOWED_FUNCTIONS (set): A set containing the names of allowed functions.

    Methods:
        visit_Import(self, node): Raises a ValueError indicating that import statements are not allowed.
        visit_ImportFrom(self, node): Raises a ValueError indicating that import from statements are not allowed.
        visit_Call(self, node): Checks if the called function is allowed. Raises a ValueError if not.
        visit_Attribute(self, node): Checks if attribute access involves dunder operations and raises an error if so.
        visit_Name(self, node): Checks if a variable name involves dunder operations and raises an error if so.
    """

    ALLOWED_FUNCTIONS = {'execute'}

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        """
        Raises a ValueError indicating that import statements are not allowed.

        Args:
            node (ast.Import): The Import node encountered during AST traversal.
        Raises:
            ValueError: Always raised to indicate that import statements are not allowed.
        """
        raise ValueError('Import statements are not allowed.')

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        """
        Raises a ValueError indicating that import from statements are not allowed.

        Args:
            node (ast.ImportFrom): The ImportFrom node encountered during AST traversal.
        Raises:
            ValueError: Always raised to indicate that import from statements are not allowed.
        """
        raise ValueError('Import from statements are not allowed.')

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        """
        Checks if the called function is allowed. Raises a ValueError if not.

        Args:
            node (ast.Call): The Call node encountered during AST traversal.
        Raises:
            ValueError: Raised if the called function is not in the allowed_functions set.
        """
        if isinstance(node.func, ast.Name) and isinstance(node.func.ctx, ast.Load):
            func_name = node.func.id
            if func_name not in self.ALLOWED_FUNCTIONS:
                raise ValueError(f'Function {func_name} is not allowed.')
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        """
        Checks if attribute access involves dunder operations and raises an error if so.

        Args:
            node (ast.Attribute): The Attribute node encountered during AST traversal.
        Raises:
            ValueError: Raised if the attribute access involves dunder operations.
        """
        if (
            isinstance(node.value, ast.Name)
            and node.attr.startswith('__')
            and node.attr.endswith('__')
        ):
            raise ValueError(f'Dunder operation {node.attr} is not allowed.')
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802
        """
        Checks if a variable name involves dunder operations and raises an error if so.

        Args:
            node (ast.Name): The Name node encountered during AST traversal.
        Raises:
            ValueError: Raised if the variable name involves dunder operations.
        """
        if (
            isinstance(node, ast.Name)
            and node.id.startswith('__')
            and node.id.endswith('__')
        ):
            raise ValueError(f'Usage of {node.id} is not allowed.')
        self.generic_visit(node)
