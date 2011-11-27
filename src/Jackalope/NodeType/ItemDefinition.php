<?php
namespace Jackalope\NodeType;

use Jackalope\Helper;
use \DOMElement;

// inherit all doc
/**
 * @api
 */
class ItemDefinition implements \PHPCR\NodeType\ItemDefinitionInterface
{
    /**
     * The factory to instantiate objects
     * @var \Jackalope\Factory
     */
    protected $factory;

    /**
     * @var NodeTypeManager
     */
    protected $nodeTypeManager;

    /**
     * Name of the declaring node type.
     * @var string
     */
    protected $declaringNodeType;
    /**
     * Name of this node type.
     * @var string
     */
    protected $name;
    /**
     * Whether this item is autocreated.
     * @var boolean
     */
    protected $isAutoCreated;
    /**
     * Whether this item is mandatory.
     * @var boolean
     */
    protected $isMandatory;
    /**
     * Whether this item is protected.
     * @var boolean
     */
    protected $isProtected;
    /**
     * On parent version constant
     * @var int
     */
    protected $onParentVersion;

    /**
     * Create a new item definition.
     *
     *  TODO: document this format. Property and Node add more to this.
     *
     * @param object $factory an object factory implementing "get" as
     *      described in \Jackalope\Factory
     * @param array $definition The property definition data as array
     * @param NodeTypeManager $nodeTypeManager
     */
    public function __construct($factory, array $definition, NodeTypeManager $nodeTypeManager)
    {
        $this->factory = $factory;
        $this->fromArray($definition);
        $this->nodeTypeManager = $nodeTypeManager;
    }

    /**
     * Load item definition from an array.
     *
     * Overwritten for property and node to add more information, with a call
     * to this parent method for the common things.
     *
     * @param array $data An array with the fields required by ItemDefition
     *
     * @return void
     */
    protected function fromArray(array $data)
    {
        $this->declaringNodeType = $data['declaringNodeType'];
        $this->name = $data['name'];
        $this->isAutoCreated = $data['isAutoCreated'];
        $this->isMandatory = $data['isMandatory'];
        $this->isProtected = $data['isProtected'];
        $this->onParentVersion = $data['onParentVersion'];
    }

    // inherit all doc
    /**
     * @api
     */
    public function getDeclaringNodeType()
    {
        return $this->nodeTypeManager->getNodeType($this->declaringNodeType);
    }

    // inherit all doc
    /**
     * @api
     */
    public function getName()
    {
        return $this->name;
    }

    // inherit all doc
    /**
     * @api
     */
    public function isAutoCreated()
    {
        return $this->isAutoCreated;
    }

    // inherit all doc
    /**
     * @api
     */
    public function isMandatory()
    {
        return $this->isMandatory;
    }

    // inherit all doc
    /**
     * @api
     */
    public function getOnParentVersion()
    {
        return $this->onParentVersion;
    }

    // inherit all doc
    /**
     * @api
     */
    public function isProtected()
    {
        return $this->isProtected;
    }
}
