
from shift_left.core.deployment_mgr import FlinkStatementNode



class DefaultDmlNameModifier():
    """
    Modifier to change the name of the dml statement
    """
    def modify_statement_name(self, node: FlinkStatementNode, statement_name: str, prefix: str) -> str:
        if prefix:
            return prefix + "-" + statement_name
        else:
            return statement_name

class DmlNameModifier(DefaultDmlNameModifier):
    """
    Modifier to change the name of the dml statement
    """
    def modify_statement_name(self, node: FlinkStatementNode,  statement_name: str, prefix: str) -> str:
        if prefix:
            statement_name = prefix + "-" + node.product_name + "-" + statement_name
        else:
            statement_name = node.product_name + "-" + statement_name
        return statement_name
