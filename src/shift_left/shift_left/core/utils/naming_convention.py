
from shift_left.core.models.flink_statement_model import FlinkStatementNode
from shift_left.core.utils.app_config import get_config


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
        if node.product_name and node.product_name != "None":
            if prefix:
                statement_name = prefix + "-" + node.product_name + "-" + statement_name
            else:
                statement_name = node.product_name + "-" + statement_name
        else:
            if prefix:
                statement_name = prefix + "-" + statement_name
        return statement_name

class DefaultComputePoolNameModifier():
    """
    Modifier to change the name of the compute pool
    """
    def modify_compute_pool_name(self, node: FlinkStatementNode, compute_pool_name: str) -> str:
        return compute_pool_name[:64]

class ComputePoolNameModifier(DefaultComputePoolNameModifier):
    """
    Modifier to change the name of the compute pool
    """
    def build_compute_pool_name_from_table(self, table_name: str) -> str:
        env = get_config().get('kafka').get('cluster_type')
        pool_name = "-".join([env, table_name.replace("_", "-")]).replace("recordconfiguration", "reccfg").replace("recordexecution", "recexe")
        return pool_name[:64]
