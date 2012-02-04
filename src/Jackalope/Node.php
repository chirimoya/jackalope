<?php

namespace Jackalope;

use ArrayIterator;
use IteratorAggregate;
use Exception;
use InvalidArgumentException;
use LogicException;

use PHPCR\PropertyType;
use PHPCR\PropertyInterface;
use PHPCR\NodeInterface;
use PHPCR\NodeType\ConstraintViolationException;
use PHPCR\RepositoryException;
use PHPCR\PathNotFoundException;
use PHPCR\ItemNotFoundException;
use PHPCR\InvalidItemStateException;
use PHPCR\ItemExistsException;

use Jackalope\Factory;

/**
 * The Node interface represents a node in a workspace.
 *
 * You can iterate over the nodes children because it is an IteratorAggregate
 *
 * @api
 */
class Node extends Item implements IteratorAggregate, NodeInterface
{
    /**
     * The index if this is a same-name sibling.
     *
     * TODO: fully implement same-name siblings
     * @var int
     */
    protected $index = 1;

    /**
     * The primary type name of this node
     * @var string
     */
    protected $primaryType;

    /**
     * mapping of property name to PropertyInterface objects.
     *
     * all properties are instantiated in the constructor
     *
     * OPTIMIZE: lazy instantiate property objects, just have local array of values
     *
     * @var array
     */
    protected $properties = array();

    /**
     * keep track of properties to be deleted until the save operation was successful.
     *
     * this is needed in order to track deletions in case of refresh
     *
     * keys are the property names, values the properties (in state deleted)
     */
    protected $deletedProperties = array();

    /**
     * list of the child node names
     * @var array
     */
    protected $nodes = array();

    /**
     * Create a new node instance with data from the storage layer
     *
     * This is only to be called by the Factory::get() method even inside the
     * Jackalope implementation to allow for custom implementations of Nodes.
     *
     * @param FactoryInterface $factory the object factory
     * @param array $rawData in the format as returned from
     *      \Jackalope\Transport\TransportInterface::getNode
     * @param string $path the absolute path of this node
     * @param Session $session
     * @param ObjectManager $objectManager
     * @param boolean $new set to true if this is a new node being created.
     *      Defaults to false which means the node is loaded from storage.
     *
     * @see \Jackalope\Transport\TransportInterface::getNode()
     *
     * @private
     */
    public function __construct(Factory $factory, $rawData, $path, Session $session, ObjectManager $objectManager, $new = false)
    {
        parent::__construct($factory, $path, $session, $objectManager, $new);
        $this->isNode = true;

        $this->parseData($rawData, false);
    }

    /**
     * Initialize or update this object with raw data from backend.
     *
     * @param array $rawData in the format as returned from Jackalope\Transport\TransportInterface
     * @param boolean $update whether to initialize this object or update
     * @param boolean $keepChanges only used if $update is true, same as $keepChanges in refresh()
     *
     * @see Node::__construct()
     * @see Node::refresh()
     */
    private function parseData($rawData, $update, $keepChanges = false)
    {
        //TODO: refactor to use hash array instead of stdClass struct

        if ($update) {
            // keep backup of old state so we can remove what needs to be removed
            $oldNodes = array_flip($this->nodes);
            $this->nodes = array(); // reset to avoid duplicates
            $oldProperties = $this->properties;
        }

        foreach ($rawData as $key => $value) {
            $node = false; // reset to avoid trouble
            if (is_object($value)) {
                // this is a node. add it unless we update and the node is deleted in this session
                if (! $update ||
                    ! $keepChanges ||
                    isset($oldNodes[$key]) || // it was here before reloading
                    ! ($node = $this->objectManager->getCachedNode($this->path . '/' . $key)) ||
                    ! $node->isDeleted()
                ) {
                    if (! $this->objectManager->isNodeMoved($this->path . '/' . $key) &&
                        ! $this->objectManager->isItemDeleted($this->path . '/' . $key)
                    ) {
                        // otherwise we (re)load a node from backend but a child has been moved away already
                        $this->nodes[] = $key;
                    }
                }
                if ($update) {
                    unset($oldNodes[$key]);
                }
            } else {
                //property or meta information

                /* Property type declarations start with :, the value then is
                 * the type string from the NodeType constants. We skip that and
                 * look at the type when we encounter the value of the property.
                 *
                 * If its a binary data, we only get the type declaration and
                 * no data. Then the $value of the type declaration is not the
                 * type string for binary, but the number of bytes of the
                 * property - resp. array of number of bytes.
                 *
                 * The magic property ::NodeIteratorSize tells this node has no
                 * children. Ignore that info for now. We might optimize with
                 * this info once we do prefetch nodes.
                 */
                if (0 === strpos($key, ':')) {
                    if ((is_int($value) || is_array($value))
                         && $key != '::NodeIteratorSize'
                    ) {
                        // This is a binary property and we just got its length with no data
                        $key = substr($key, 1);
                        if (!isset($rawData->$key)) {
                            $binaries[$key] = $value;
                            if ($update) {
                                unset($oldProperties[$key]);
                            }
                            if (isset($this->properties[$key])) {
                                // refresh existing binary, this will only happen in update
                                // only update length
                                if (! ($keepChanges && $this->properties[$key]->isModified())) {
                                    $this->properties[$key]->_setLength($value);
                                    if ($this->properties[$key]->isDirty()) {
                                        $this->properties[$key]->setClean();
                                    }
                                }
                            } else {
                                // this will always fall into the creation mode
                                $this->_setProperty($key, $value, PropertyType::BINARY, true);
                            }
                        }
                    } //else this is a type declaration

                    //skip this entry (if its binary, its already processeed
                    continue;
                }

                if ($update && array_key_exists($key, $this->properties)) {
                    unset($oldProperties[$key]);
                    $prop = $this->properties[$key];
                    if ($keepChanges && $prop->isModified()) {
                        continue;
                    }
                } elseif ($update && array_key_exists($key, $this->deletedProperties)) {
                    if ($keepChanges) {
                        // keep the delete
                        continue;
                    } else {
                        // restore the property
                        $this->properties[$key] = $this->deletedProperties[$key];
                        $this->properties[$key]->setClean();
                        // now let the loop update the value. no need to talk to ObjectManager as it
                        // does not store property deletions
                    }
                }

                switch ($key) {
                    case 'jcr:index':
                        $this->index = $value;
                        break;
                    case 'jcr:primaryType':
                        $this->primaryType = $value;
                        // type information is exposed as property too, although there exist more specific methods
                        $this->_setProperty('jcr:primaryType', $value, PropertyType::NAME, true);
                        break;
                    case 'jcr:mixinTypes':
                        // type information is exposed as property too, although there exist more specific methods
                        $this->_setProperty($key, $value, PropertyType::NAME, true);
                        break;

                    // OPTIMIZE: do not instantiate properties until needed
                    default:
                        if (isset($rawData->{':' . $key})) {
                            /*
                             * this is an inconsistency between jackrabbit and
                             * dbal transport: jackrabbit has type name, dbal
                             * delivers numeric type.
                             * we should eventually fix the format returned by
                             * transport and either have jackrabbit transport
                             * do the conversion or let dbal store a string
                             * value instead of numerical.
                             */
                            $type = is_numeric($rawData->{':' . $key})
                                    ? $rawData->{':' . $key}
                                    : PropertyType::valueFromName($rawData->{':' . $key});
                        } else {
                            $type = PropertyType::determineType(is_array($value) ? reset($value) : $value);
                        }
                        $this->_setProperty($key, $value, $type, true);
                        break;
                }
            }
        }

        if ($update) {
            // notify nodes that where not received again that they disappeared
            foreach ($oldNodes as $name => $dummy) {
                if (! $this->objectManager->purgeDisappearedNode($this->path . '/' . $name, $keepChanges)) {
                    // do not drop, it was a new child and we are to keep changes
                    $this->nodes[] = $name;
                } else {
                    $id = array_search($name, $this->nodes);
                    if ($id) {
                        unset($this->nodes[$id]);
                    }
                }
            }
            foreach ($oldProperties as $name => $property) {
                if (! ($property->isNew() && $keepChanges)) {
                    // may not call remove(), we dont want another delete with the backend to be attempted
                    $this->properties[$name]->setDeleted();
                    unset($this->properties[$name]);
                }
            }
        }
    }

    /**
     * Creates a new node at the specified $relPath
     *
     * {@inheritDoc}
     *
     * In Jackalope, the child node type definition is immediatly applied if no
     * primaryNodeTypeName is specified.
     *
     * The PathNotFoundException and ConstraintViolationException are thrown immediatly.
     * Version and Lock are delayed until save.
     *
     * @api
     */
    public function addNode($relPath, $primaryNodeTypeName = null)
    {
        $ntm = $this->session->getWorkspace()->getNodeTypeManager();

        // are we not the immediate parent?
        if (strpos($relPath, '/') !== false) {
            // forward to real parent
            try {
                $parentNode = $this->objectManager->getNode(dirname($relPath), $this->path);
            } catch(ItemNotFoundException $e) {
                try {
                    //we have to throw a different exception if there is a property with that name than if there is nothing at the path at all. lets see if the property exists
                    $prop = $this->objectManager->getPropertyByPath($this->getChildPath(dirname($relPath)));
                    if (! is_null($prop)) {
                        throw new ConstraintViolationException('Not allowed to add a node below a property');
                    }
                } catch(ItemNotFoundException $e) {
                    //ignore to throw the PathNotFoundException below
                }

                throw new PathNotFoundException($e->getMessage(), $e->getCode(), $e);
            }
            return $parentNode->addNode(basename($relPath), $primaryNodeTypeName);
        }

        if (!is_null($primaryNodeTypeName)) {
            // sanitize
            $nt = $ntm->getNodeType($primaryNodeTypeName);
            if ($nt->isMixin()) {
                throw new ConstraintViolationException('Not allowed to add a node with a mixin type: '.$primaryNodeTypeName);
            } elseif ($nt->isAbstract()) {
                throw new ConstraintViolationException('Not allowed to add a node with an abstract type: '.$primaryNodeTypeName);
            }
        } else {
            if ($this->primaryType === 'rep:root') {
                $primaryNodeTypeName = 'nt:unstructured';
            } else {
                $type = $ntm->getNodeType($this->primaryType);
                $nodeDefinitions = $type->getChildNodeDefinitions();
                foreach ($nodeDefinitions as $def) {
                    if (!is_null($def->getDefaultPrimaryType())) {
                        $primaryNodeTypeName = $def->getDefaultPrimaryTypeName();
                        break;
                    }
                }
                if (is_null($primaryNodeTypeName)) {
                    throw new ConstraintViolationException("No matching child node definition found for `$relPath' in type `{$this->primaryType}'. Please specify the type explicitly.");
                }
            }
        }

        // create child node
        //sanity check: no index allowed. TODO: we should verify this is a valid node name
        if (false !== strpos($relPath, ']')) {
            throw new RepositoryException("Index not allowed in name of newly created node: $relPath");
        }
        if (in_array($relPath, $this->nodes)) {
            throw new ItemExistsException("This node already has a child named $relPath."); //TODO: same-name siblings if nodetype allows for them
        }
        $data = array('jcr:primaryType' => $primaryNodeTypeName);
        $path = $this->getChildPath($relPath);
        $node = $this->factory->get('Node', array($data, $path, $this->session, $this->objectManager, true));
        $this->objectManager->addItem($path, $node);
        $this->nodes[] = $relPath;
        //by definition, adding a node sets the parent to modified
        $this->setModified();

        return $node;
    }

    /**
     * Jackalope implements this feature and updates the position of the
     * existing child at srcChildRelPath to be in the list immediately before
     * destChildRelPath.
     *
     * {@inheritDoc}
     *
     * Jackalope has no implementation-specific ordering restriction so no
     * \PHPCR\ConstraintViolationException is expected. VersionException and
     * LockException are not tested immediatly but thrown on save.
     *
     * TODO: Make the backend actually pick up the move
     *
     * @api
     */
    public function orderBefore($srcChildRelPath, $destChildRelPath)
    {
        if ($srcChildRelPath == $destChildRelPath) {
            //nothing to move
            return;
        }
        $oldpos = array_search($srcChildRelPath, $this->nodes);
        if ($oldpos === false) {
            throw new ItemNotFoundException("$srcChildRelPath is not a valid child of ".$this->path);
        }

        if ($destChildRelPath == null) {
            //null means move to end
            unset($this->nodes[$oldpos]);
            $this->nodes[] = $srcChildRelPath;
        } else {
            //insert somewhere specified by dest path
            $newpos = array_search($destChildRelPath, $this->nodes);
            if ($newpos === false) {
                throw new ItemNotFoundException("$destChildRelPath is not a valid child of ".$this->path);
            }
            if ($oldpos < $newpos) {
                //we first unset, so
                $newpos--;
            }
            unset($this->nodes[$oldpos]);
            array_splice($this->nodes, $newpos, 0, $srcChildRelPath);
        }
        $this->setModified();
        //TODO: this is not enough to persist the reordering with the transport
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function setProperty($name, $value, $type = PropertyType::UNDEFINED)
    {
        $this->checkState();

        //validity check property allowed (or optional, for remove) will be done by backend on commit, which is allowed by spec

        if (is_null($value)) {
            if (isset($this->properties[$name])) {
                $this->properties[$name]->remove();
            }
            return null;
        }

        return $this->_setProperty($name, $value, $type, false);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getNode($relPath)
    {
        $this->checkState();

        try {
            $node = $this->objectManager->getNodeByPath($this->objectManager->absolutePath($this->path, $relPath));
        } catch (ItemNotFoundException $e) {
            throw new PathNotFoundException($e->getMessage(), $e->getCode(), $e);
        }
        return $node;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getNodes($filter = null)
    {
        $this->checkState();

        $names = self::filterNames($filter, $this->nodes);
        $paths = $pathNameMap = $result = array();
        if (!empty($names)) {
            foreach ($names as $name) {
                $result[$name] = $this->getNode($name);
                $path = $this->objectManager->absolutePath($this->path, $name);
                $paths[] = $path;
                $pathNameMap[$path] = $name;
            }

            $nodes = $this->objectManager->getNodesByPath($paths);
            foreach ($nodes as $path => $node) {
                $result[$pathNameMap[$path]] = $node;
            }
        }

        return new ArrayIterator($result);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getProperty($relPath)
    {
        $this->checkState();

        if (false === strpos($relPath, '/')) {
            if (!isset($this->properties[$relPath])) {
                throw new PathNotFoundException("Property $relPath in ".$this->path);
            }

            return $this->properties[$relPath];
        }

        return $this->session->getProperty($this->getChildPath($relPath));
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getPropertyValue($name, $type=null)
    {
        $this->checkState();

        $val = $this->getProperty($name)->getValue();
        if (! is_null($type)) {
            $val = PropertyType::convertType($val, $type);
        }
        return $val;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getProperties($filter = null)
    {
        $this->checkState();

        //OPTIMIZE: lazy iterator?
        $names = self::filterNames($filter, array_keys($this->properties));
        $result = array();
        foreach ($names as $name) {
            $result[$name] = $this->properties[$name]; //we know for sure the properties exist, as they come from the array keys of the array we are accessing
        }
        return new ArrayIterator($result);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getPropertiesValues($filter=null, $dereference=true)
    {
        $this->checkState();

        // OPTIMIZE: do not create properties in constructor, go over array here
        $names = self::filterNames($filter, array_keys($this->properties));
        $result = array();
        foreach ($names as $name) {
            //we know for sure the properties exist, as they come from the array keys of the array we are accessing
            $type = $this->properties[$name]->getType();
            if (! $dereference &&
                    (PropertyType::REFERENCE == $type
                    || PropertyType::WEAKREFERENCE == $type
                    || PropertyType::PATH == $type)
            ) {
                $result[$name] = $this->properties[$name]->getString();
            } else {
                // OPTIMIZE: collect the paths and call objectmanager->getNodesByPath once
                $result[$name] = $this->properties[$name]->getValue();
            }
        }
        return $result;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getPrimaryItem()
    {
        try {
            $primary_item = null;
            $mgr = $this->session->getWorkspace()->getNodeTypeManager();
            $item_name = $this->getPrimaryNodeType()->getPrimaryItemName();

            if ($item_name !== null) {
                $primary_item = $this->session->getItem($this->path . '/' . $item_name);
            }
        } catch (Exception $ex) {
            throw new RepositoryException("An error occured while reading the primary item of the node '{$this->path}': " . $ex->getMessage());
        }

        if ($primary_item === null) {
           throw new ItemNotFoundException("No primary item found for node '{$this->path}'");
        }

        return $primary_item;
    }


    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getIdentifier()
    {
        $this->checkState();

        if (isset($this->properties['jcr:uuid'])) {
            return $this->getPropertyValue('jcr:uuid');
        }
        return null;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getIndex()
    {
        $this->checkState();

        return $this->index;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getReferences($name = null)
    {
        $this->checkState();

        return $this->objectManager->getReferences($this->path, $name);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getWeakReferences($name = null)
    {
        $this->checkState();

        return $this->objectManager->getWeakReferences($this->path, $name);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function hasNode($relPath)
    {
        $this->checkState();

        if (false === strpos($relPath, '/')) {
            return array_search($relPath, $this->nodes) !== false;
        }
        if (! strlen($relPath) || $relPath[0] == '/') {
            throw new InvalidArgumentException("'$relPath' is not a relative path");
        }

        return $this->session->nodeExists($this->getChildPath($relPath));
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function hasProperty($relPath)
    {
        $this->checkState();

        if (false === strpos($relPath, '/')) {
            return isset($this->properties[$relPath]);
        }
        if (! strlen($relPath) || $relPath[0] == '/') {
            throw new InvalidArgumentException("'$relPath' is not a relative path");
        }

        return $this->session->propertyExists($this->getChildPath($relPath));
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function hasNodes()
    {
        $this->checkState();

        return !empty($this->nodes);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function hasProperties()
    {
        $this->checkState();

        return (! empty($this->properties));
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getPrimaryNodeType()
    {
        $this->checkState();

        $ntm = $this->session->getWorkspace()->getNodeTypeManager();
        return $ntm->getNodeType($this->primaryType);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getMixinNodeTypes()
    {
        $this->checkState();

        if (!isset($this->properties['jcr:mixinTypes'])) {
            return array();
        }
        $res = array();
        $ntm = $this->session->getWorkspace()->getNodeTypeManager();
        foreach ($this->properties['jcr:mixinTypes']->getValue() as $type) {
            $res[] = $ntm->getNodeType($type);
        }
        return $res;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function isNodeType($nodeTypeName)
    {
        $this->checkState();

        // is it the primary type?
        if ($this->primaryType == $nodeTypeName) {
            return true;
        }
        $ntm = $this->session->getWorkspace()->getNodeTypeManager();
        // is the primary type a subtype of the type?
        if ($ntm->getNodeType($this->primaryType)->isNodeType($nodeTypeName)) {
            return true;
        }
        // if there are no mixin types, then we now know this node is not of that type
        if (! isset($this->properties["jcr:mixinTypes"])) {
            return false;
        }
        // is it one of the mixin types?
        if (in_array($nodeTypeName, $this->properties["jcr:mixinTypes"]->getValue())) {
            return true;
        }
        // is it an ancestor of any of the mixin types?
        foreach($this->properties['jcr:mixinTypes'] as $mixin) {
            if ($ntm->getNodeType($mixin)->isNodeType($nodeTypeName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Changes the primary node type of this node to nodeTypeName.
     *
     * {@inheritDoc}
     *
     * Jackalope only validates type conflicts on save.
     *
     * @api
     */
    public function setPrimaryType($nodeTypeName)
    {
        $this->checkState();

        throw new NotImplementedException('Write');
    }

    /**
     * {@inheritDoc}
     *
     * Jackalope validates type conflicts only on save, not immediatly.
     *It is possible to add mixin types after the first save.
     *
     * @api
     */
    public function addMixin($mixinName)
    {
        // Check if mixinName exists as a mixin type
        $typemgr = $this->session->getWorkspace()->getNodeTypeManager();
        $nodeType = $typemgr->getNodeType($mixinName);
        if (! $nodeType->isMixin()) {
            throw new ConstraintViolationException("Trying to add a mixin '$mixinName' that is a primary type");
        }

        $this->checkState();

        // TODO handle LockException & VersionException cases
        if ($this->hasProperty('jcr:mixinTypes')) {
            if (array_search($mixinName, $this->properties['jcr:mixinTypes']->getValue()) === false) {
                $this->properties['jcr:mixinTypes']->addValue($mixinName);
                $this->setModified();
            }
        } else {
            $this->setProperty('jcr:mixinTypes', array($mixinName), PropertyType::NAME);
            $this->setModified();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function removeMixin($mixinName)
    {
        $this->checkState();

        // check if node type is assigned

        $this->setModified();

        throw new NotImplementedException('Write');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function canAddMixin($mixinName)
    {
        $this->checkState();

        throw new NotImplementedException('Write');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getDefinition()
    {
        $this->checkState();

        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function update($srcWorkspace)
    {
        $this->checkState();

        if ($this->isNew()) {
            //no node in workspace
            return;
        }

        throw new NotImplementedException('Write');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getCorrespondingNodePath($workspaceName)
    {
        $this->checkState();

        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getSharedSet()
    {
        $this->checkState();

        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function removeSharedSet()
    {
        $this->checkState();
        $this->setModified();

        throw new NotImplementedException('Write');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function removeShare()
    {
        $this->checkState();
        $this->setModified();

        throw new NotImplementedException('Write');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function isCheckedOut()
    {
        $this->checkState();

        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function isLocked()
    {
        $this->checkState();

        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function followLifecycleTransition($transition)
    {
        $this->checkState();
        $this->setModified();

        throw new NotImplementedException('Write');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getAllowedLifecycleTransitions()
    {
        $this->checkState();

        throw new NotImplementedException('Write');
    }

    /**
     * Refresh this node
     *
     * {@inheritDoc}
     *
     * This is also called internally to refresh when the node is accessed in
     * state DIRTY.
     *
     * @param boolean $keepChanges whether to keep local changes
     * @param boolean $internal implementation internal flag to not check for the InvalidItemStateException
     *
     * @see Item::checkState
     *
     * @api
     */
    public function refresh($keepChanges, $internal = false)
    {
        if (! $internal && $this->isDeleted()) {
            throw new InvalidItemStateException('This item has been removed and can not be refreshed');
        }
        $deleted = false;

        // Get properties and children from backend
        try {
            $json = $this->objectManager->getTransport()->getNode(is_null($this->oldPath) ? $this->path : $this->oldPath);
        } catch(ItemNotFoundException $ex) {

            // The node was deleted in another session
            if (! $this->objectManager->purgeDisappearedNode($this->path, $keepChanges)) {
                throw new LogicException($this->path . " should be purged and not kept");
            }
            $keepChanges = false; // delete never keeps changes
            if (! $internal) {
                // this is not an internal update
                $deleted = true;
            }

            // continue with emtpy data, parseData will notify all cached
            // children and all properties that we are removed
            $json = array();
        }

        $this->parseData($json, true, $keepChanges);

        if ($deleted) {
            $this->setDeleted();
        }
    }

    /**
     * Remove this node
     *
     * {@inheritDoc}
     *
     * A jackalope node needs to notify the parent node about this if it is
     * cached, in addition to \PHPCR\ItemInterface::remove()
     *
     * @uses Node::unsetChildNode()
     *
     * @api
     */
    public function remove()
    {
        $this->checkState();
        $parent = $this->objectManager->getCachedNode($this->parentPath);
        if ($parent) {
            $parent->unsetChildNode($this->name, true);
        }
        // once we removed ourselves, $this->getParent() won't work anymore. do this last
        parent::remove();
    }

    /**
     * Removes the reference in the internal node storage
     *
     * @param string $name the name of the child node to unset
     * @param bool $check whether a state check should be done - set to false
     *      during internal update operations
     *
     * @return void
     *
     * @throws ItemNotFoundException If there is no child with $name
     *
     * @private
     */
    public function unsetChildNode($name, $check)
    {
        if ($check) {
            $this->checkState();
        }

        $key = array_search($name, $this->nodes);
        if ($key === false) {
            if (! $check) {
                // inside a refresh operation
                return;
            }
            throw new ItemNotFoundException("Could not remove child node because it's already gone");
        }

        unset($this->nodes[$key]);
    }

    /**
     * Adds child node to this node for internal reference
     *
     * @param string $name The name of the child node
     * @param boolean $check whether to check state
     *
     * @private
     */
    public function addChildNode($name, $check)
    {
        if ($check) {
            $this->checkState();
        }

        // TODO: same name siblings

        $this->nodes[] = $name;
    }

    /**
     * Removes the reference in the internal node storage
     *
     * @param string $name the name of the property to unset.
     *
     * @return void
     *
     * @throws ItemNotFoundException If this node has no property with name $name
     *
     * @private
     */
    public function unsetProperty($name)
    {
        $this->checkState();
        $this->setModified();

        if (!array_key_exists($name, $this->properties)) {
            throw new ItemNotFoundException('Implementation Error: Could not remove property from node because it is already gone');
        }
        $this->deletedProperties[$name] = $this->properties[$name];
        unset($this->properties[$name]);
    }

    /**
     * In addition to calling parent method, clean deletedProperties
     */
    public function confirmSaved()
    {
        foreach($this->properties as $property) {
            if ($property->isModified()) {
                $property->confirmSaved();
            }
        }
        $this->deletedProperties = array();
        parent::confirmSaved();
    }

    /**
     * Make sure $p is an absolute path
     *
     * If its a relative path, prepend the path to this node, otherwise return as is
     *
     * @param string $p the relative or absolute property or node path
     *
     * @return string the absolute path to this item, with relative paths resolved against the current node
     */
    protected function getChildPath($p)
    {
        if ('' == $p) {
            throw new InvalidArgumentException("Name can not be empty");
        }
        if ($p[0] == '/') {
            return $p;
        }
        //relative path, combine with base path for this node
        $path = $this->path === '/' ? '/' : $this->path.'/';
        return $path . $p;
    }

    /**
     * Filter the list of names according to the filter expression / array
     *
     * @param string|array $filter according to getNodes|getProperties
     * @param array $names list of names to filter
     *
     * @return the names in $names that match a filter
     */
    protected static function filterNames($filter, $names)
    {
        if (is_string($filter)) {
            $filter = explode('|', $filter);
        }
        $filtered = array();
        if ($filter !== null) {
            foreach ($filter as $k => $f) {
               $f = trim($f);
               $filter[$k] = strtr($f, array('*'=>'.*', //wildcard
                                             '.'  => '\\.', //escape regexp
                                             '\\' => '\\\\',
                                             '{'  => '\\{',
                                             '}'  => '\\}',
                                             '('  => '\\(',
                                             ')'  => '\\)',
                                             '+'  => '\\+',
                                             '^'  => '\\^',
                                             '$'  => '\\$'));
            }
            foreach ($names as $name) {
                foreach ($filter as $f) {
                    if (preg_match('/^'.$f.'$/', $name)) {
                        $filtered[] = $name;
                    }
                }
            }
        } else {
            $filtered = $names;
        }
        return $filtered;
    }

    /**
     * Provide Traversable interface: redirect to getNodes with no filter
     *
     * @return Iterator over all child nodes
     */
    public function getIterator()
    {
        $this->checkState();

        return $this->getNodes();
    }

    /**
     * Implement really setting the property without any notification.
     *
     * Implement the setProperty, but also used from constructor or in refresh,
     * when the backend has a new property that is not yet loaded in memory.
     *
     * @param string $name
     * @param mixed $value
     * @param string $type
     * @param boolean $internal whether we are setting this node through api or internally
     *
     * @return Property
     *
     * @see Node::setProperty
     * @see Node::refresh
     * @see Node::__construct
     */
    protected function _setProperty($name, $value, $type, $internal)
    {
        if ($name == '' | false !== strpos($name, '/')) {
            throw new InvalidArgumentException("The name '$name' is no valid property name");
        }

        if (!isset($this->properties[$name])) {
            $path = $this->getChildPath($name);
            $property = $this->factory->get(
                            'Property',
                            array(array('type' => $type, 'value' => $value),
                                  $path,
                                  $this->session,
                                  $this->objectManager,
                                  ! $internal));
            $this->properties[$name] = $property;
            if (! $internal) {
                $this->setModified();
                $this->objectManager->addItem($path, $property);
            }
        } else {
            if ($internal) {
                $this->properties[$name]->_setValue($value, $type);
                if ($this->properties[$name]->isDirty()) {
                    $this->properties[$name]->setClean();
                }
            } else {
                $this->properties[$name]->setValue($value, $type);
            }
        }
        return $this->properties[$name];
    }

    /**
     * In addition to set this item deleted, set all properties to deleted.
     *
     * They will be automatically deleted by the backend, but the user might
     * still have a reference to one of the property objects.
     */
    public function setDeleted()
    {
        parent::setDeleted();
        foreach ($this->properties as $property) {
            $property->setDeleted(); // not all properties are tracked in objectmanager
        }
    }

    /**
     * {@inheritDoc}
     *
     * Additionally, notifies all properties of this node. Child nodes are not
     * notified, it is the job of the ObjectManager to know which nodes are
     * cached and notify them.
     */
    public function beginTransaction()
    {
        parent::beginTransaction();

        // Notify the children properties
        foreach ($this->properties as $prop) {
            $prop->beginTransaction();
        }
    }

    /**
     * {@inheritDoc}
     *
     * Additionally, notifies all properties of this node. Child nodes are not
     * notified, it is the job of the ObjectManager to know which nodes are
     * cached and notify them.
     */
    public function commitTransaction()
    {
        parent::commitTransaction();

        foreach ($this->properties as $prop) {
            $prop->commitTransaction();
        }
    }

    /**
     * {@inheritDoc}
     *
     * Additionally, notifies all properties of this node. Child nodes are not
     * notified, it is the job of the ObjectManager to know which nodes are
     * cached and notify them.
     */
    public function rollbackTransaction()
    {
        parent::rollbackTransaction();

        foreach ($this->properties as $prop) {
            $prop->rollbackTransaction();
        }
    }

}
