<?php
namespace Jackalope\Transport\MongoDB\Query;

use PHPCR\NodeType\NodeTypeManagerInterface;
use PHPCR\Query\QOM;

/**
 * Converts QOM to Query Statements for the MongoDB backend.
 *
 * @license http://www.apache.org/licenses/LICENSE-2.0  Apache License Version 2.0, January 2004
 */
class QOMWalker
{
    /**
     * @var NodeTypeManagerInterface
     */
    private $nodeTypeManager;
    
    /**
     * @var \Doctrine\MongoDB\Query\Builder
     */
    private $qb;

    /**
     * @var array
     */
    private $alias = array();

    /**
     * @var array
     */
    private $namespaces;

    public function __construct(NodeTypeManagerInterface $manager, \Doctrine\MongoDB\Query\Builder $qb, array $namespaces = array())
    {
        $this->nodeTypeManager = $manager;
        $this->qb = $qb;
        $this->namespaces = $namespaces;
    }
    
    public function walkQOMQuery(QOM\QueryObjectModelInterface $qom)
    {
        $this->walkColumns($qom->getColumns());
        
        $this->walkSource($qom->getSource());
        
        $contraint = $qom->getConstraint();
        if ($contraint) {
            $this->walkConstraint($contraint);
        }
        
        $orderings = $qom->getOrderings();
        if ($orderings) {
            $this->walkOrderings($orderings);
        }
    }

    public function walkColumns($columns)
    {
        if ($columns) {
            $cols = array();
            foreach ($columns as $column) {
                $cols[] = $this->walkColumn($column);
            }
            $this->qb->select($cols);
        }
    }

    public function walkColumn(QOM\ColumnInterface $column)
    {
        // TODO
    }

    public function walkSource(QOM\SourceInterface $source)
    {
        if (!($source instanceof QOM\SelectorInterface)) {
            throw new \Jackalope\NotImplementedException("Only Selector Sources are supported.");
        }

        $types = array($source->getNodeTypeName());
        $subTypes = $this->nodeTypeManager->getSubtypes($source->getNodeTypeName());
        foreach ($subTypes as $subType) {
            $types[] = $subType;
        }

        $this->qb->field('type')->in($types);
    }

    public function walkConstraint(QOM\ConstraintInterface $constraint)
    {
        if ($constraint instanceof QOM\AndInterface) {
            return $this->walkAndConstraint($constraint);
        } else if ($constraint instanceof QOM\OrInterface) {
            return $this->walkOrConstraint($constraint);
        } else if ($constraint instanceof QOM\NotInterface) {
            return $this->walkNotConstraint($constraint);
        } else if ($constraint instanceof QOM\ComparisonInterface) {
            return $this->walkComparisonConstraint($constraint);
        } else if ($constraint instanceof QOM\DescendantNodeInterface) {
            return $this->walkDescendantNodeConstraint($constraint);
        } else if ($constraint instanceof QOM\ChildNodeInterface) {
            return $this->walkChildNodeConstraint($constraint);
        } else if ($constraint instanceof QOM\PropertyExistenceInterface) {
            return $this->walkPropertyExistanceConstraint($constraint);
        } else if ($constraint instanceof QOM\SameNodeInterface) {
            return $this->walkSameNodeConstraint($constraint);
        } else {
            throw new \PHPCR\Query\InvalidQueryException("Constraint " . get_class($constraint) . " not yet supported.");
        }
    }

    public function walkSameNodeConstraint(QOM\SameNodeInterface $constraint)
    {
        return $this->getTableAlias($constraint->getSelectorName()) . ".path = '" . $constraint->getPath() . "'";
    }

    /**
     *
     * @param QOM\PropertyExistenceInterface $constraint
     */
    public function walkPropertyExistanceConstraint(QOM\PropertyExistenceInterface $constraint)
    {
        return $this->sqlXpathValueExists($this->getTableAlias($constraint->getSelectorName()), $constraint->getPropertyName());
    }

    /**
     * @param QOM\DescendantNodeInterface $constraint
     * @return string
     */
    public function walkDescendantNodeConstraint(QOM\DescendantNodeInterface $constraint)
    {
        return $this->getTableAlias($constraint->getSelectorName()) . ".path LIKE '" . $constraint->getAncestorPath() . "/%'";
    }

    public function walkChildNodeConstraint(QOM\ChildNodeInterface $constraint)
    {
        return $this->getTableAlias($constraint->getSelectorName()) . ".parent = '" . $constraint->getParentPath() . "'";
    }

    /**
     * @param QOM\AndInterface $constraint
     * @return string
     */
    public function walkAndConstraint(QOM\AndInterface $constraint)
    {
        return "(" . $this->walkConstraint($constraint->getConstraint1()) . " AND " . $this->walkConstraint($constraint->getConstraint2()) . ")";
    }

    /**
     * @param QOM\OrInterface $constraint
     * @return string
     */
    public function walkOrConstraint(QOM\OrInterface $constraint)
    {
        return "(" . $this->walkConstraint($constraint->getConstraint1()) . " OR " . $this->walkConstraint($constraint->getConstraint2()) . ")";
    }

    /**
     * @param QOM\NotInterface $constraint
     * @return string
     */
    public function walkNotConstraint(QOM\NotInterface $constraint)
    {
        return "NOT (" . $this->walkConstraint($constraint->getConstraint()) . ")";
    }

    /**
     * @param QOM\ComparisonInterface $constraint
     */
    public function walkComparisonConstraint(QOM\ComparisonInterface $constraint)
    {
        $this->walkOperator($constraint->getOperator(), $this->walkOperand($constraint->getOperand1()), $this->walkOperand($constraint->getOperand2()));
    }

    /**
     * @param string $operator
     * @return string
     */
    public function walkOperator($operator, $field, $value)
    {
        if ($field == 'jcr:path') {
            $field = 'path';
        } else if ($field == "jcr:uuid") {
            $field = '_id';
        } else {
            $this->qb->field('props.name')->equals($field);
            $field = 'props.value';
        }
            
        if ($operator == QOM\QueryObjectModelConstantsInterface::JCR_OPERATOR_EQUAL_TO) {
            $this->qb->field($field)->equals($value);
        } else if ($operator == QOM\QueryObjectModelConstantsInterface::JCR_OPERATOR_GREATER_THAN) {
            return ">";
        } else if ($operator == QOM\QueryObjectModelConstantsInterface::JCR_OPERATOR_GREATER_THAN_OR_EQUAL_TO) {
            return ">=";
        } else if ($operator == QOM\QueryObjectModelConstantsInterface::JCR_OPERATOR_LESS_THAN) {
            return "<";
        } else if ($operator == QOM\QueryObjectModelConstantsInterface::JCR_OPERATOR_LESS_THAN_OR_EQUAL_TO) {
            return "<=";
        } else if ($operator == QOM\QueryObjectModelConstantsInterface::JCR_OPERATOR_NOT_EQUAL_TO) {
            return "!=";
        } else if ($operator == QOM\QueryObjectModelConstantsInterface::JCR_OPERATOR_LIKE) {
            return "LIKE";
        } else {
            return $operator; // no-op for simplicity, not standard conform (but using the constants is a pain)
        }
    }

    /**
     * @param QOM\OperandInterface $operand
     */
    public function walkOperand(QOM\OperandInterface $operand)
    {
        if ($operand instanceof QOM\NodeNameInterface) {
            $selector = $operand->getSelectorName();
            $alias = $this->getTableAlias($selector);

            return $alias.".local_name"; // TODO: Hm, what about the namespace?
        } else if ($operand instanceof QOM\NodeLocalNameInterface) {
            $selector = $operand->getSelectorName();
            $alias = $this->getTableAlias($selector);

            return $alias.".local_name";
        } else if ($operand instanceof QOM\LowerCaseInterface) {
            return $this->platform->getLowerExpression($this->walkOperand($operand->getOperand()));
        } else if ($operand instanceof QOM\UpperCaseInterface) {
            return $this->platform->getUpperExpression($this->walkOperand($operand->getOperand()));
        } else if ($operand instanceof QOM\LiteralInterface) {
            return $operand->getLiteralValue();
        } else if ($operand instanceof QOM\PropertyValueInterface) {
            return $operand->getPropertyName();
        } else if ($operand instanceof QOM\LengthInterface) {

            $alias = $this->getTableAlias($operand->getPropertyValue()->getSelectorName());
            $property = $operand->getPropertyValue()->getPropertyName();
            if ($property == "jcr:path") {
                return "path";
            } else if ($property == "jcr:uuid") {
                return "_id";
            } else {
                return $property;
            }

        } else {
            throw new \PHPCR\Query\InvalidQueryException("Dynamic operand " . get_class($operand) ." not yet supported.");
        }
    }

    public function walkOrderings(array $orderings)
    {
        $sql = "ORDER BY ";
        foreach ($orderings AS $ordering) {
            $sql .= $this->walkOrdering($ordering);
        }
        return $sql;
    }

    public function walkOrdering(QOM\OrderingInterface $ordering)
    {
        return $this->walkOperand($ordering->getOperand()) . " " .
               (($ordering->getOrder() == QOM\QueryObjectModelConstantsInterface::JCR_ORDER_ASCENDING) ? "ASC" : "DESC");
    }

    /**
     * SQL to execute an XPATH expression checking if the property exist on the node with the given alias.
     *
     * @param string $alias
     * @param string $property
     * @return string
     */
    private function sqlXpathValueExists($alias, $property)
    {
        if ($this->platform instanceof \Doctrine\DBAL\Platforms\MySqlPlatform) {
            return "EXTRACTVALUE($alias.props, 'count(//sv:property[@sv:name=\"" . $property . "\"]/sv:value[1])') = 1";
        } else if ($this->platform instanceof \Doctrine\DBAL\Platforms\PostgreSqlPlatform) {
            return "xpath_exists('//sv:property[@sv:name=\"" . $property . "\"]/sv:value[1]', CAST($alias.props AS xml), ".$this->sqlXpathPostgreSQLNamespaces().") = 't'";
        } else {
            throw new \Jackalope\NotImplementedException("Xpath evaluations cannot be executed with '" . $this->platform->getName() . "' yet.");
        }
    }

    /**
     * SQL to execute an XPATH expression extracting the property value on the node with the given alias.
     *
     * @param string $alias
     * @param string $property
     * @return string
     */
    private function sqlXpathExtractValue($alias, $property)
    {
        if ($this->platform instanceof \Doctrine\DBAL\Platforms\MySqlPlatform) {
            return "EXTRACTVALUE($alias.props, '//sv:property[@sv:name=\"" . $property . "\"]/sv:value[1]')";
        } else if ($this->platform instanceof \Doctrine\DBAL\Platforms\PostgreSqlPlatform) {
            return "(xpath('//sv:property[@sv:name=\"" . $property . "\"]/sv:value[1]/text()', CAST($alias.props AS xml), ".$this->sqlXpathPostgreSQLNamespaces()."))[1]::text";
        } else {
            throw new \Jackalope\NotImplementedException("Xpath evaluations cannot be executed with '" . $this->platform->getName() . "' yet.");
        }
    }

    private function sqlXpathPostgreSQLNamespaces()
    {
        $namespaces = "ARRAY[ARRAY['sv', 'http://www.jcp.org/jcr/sv/1.0']]";
        return $namespaces;
    }
}