<?php

require_once __DIR__.'/../phpcr-api/inc/AbstractLoader.php';

/**
 * Implementation loader for jackalope-mongodb
 */
class ImplementationLoader extends \PHPCR\Test\AbstractLoader
{
    private static $instance = null;

    private $necessaryConfigValues = array('phpcr.user', 'phpcr.pass', 'phpcr.workspace');
    
    protected function __construct()
    {
        // Make sure we have the necessary config
        foreach ($this->necessaryConfigValues as $val) {
            if (empty($GLOBALS[$val])) {
                die('Please set '.$val.' in your phpunit.xml.' . "\n");
            }
        }
        
        parent::__construct('Jackalope\RepositoryFactoryMongoDB');

        $this->unsupportedChapters = array(
                    'Query',
                    'Export',
                    'NodeTypeDiscovery',
                    'PermissionsAndCapabilities',
                    'Import',
                    'Observation',
                    'WorkspaceManagement',
                    'ShareableNodes',
                    'Versioning',
                    'AccessControlManagement',
                    'Locking',
                    'LifecycleManagement',
                    'NodeTypeManagement',
                    'RetentionAndHold',
                    'Transactions',
                    'SameNameSiblings',
                    'OrderableChildNodes',
        );

        $this->unsupportedCases = array();

        $this->unsupportedTests = array(
                    'Connecting\\RepositoryTest::testLoginException', //TODO: figure out what would be invalid credentials
                    'Connecting\\RepositoryTest::testNoLogin',
                    'Connecting\\RepositoryTest::testNoLoginAndWorkspace',

                    'Reading\\SessionReadMethodsTest::testImpersonate', //TODO: Check if that's implemented in newer jackrabbit versions.
                    'Reading\\SessionNamespaceRemappingTest::testSetNamespacePrefix',
                    'Reading\\NodeReadMethodsTest::testGetSharedSetUnreferenced', // TODO: should this be moved to 14_ShareableNodes
        
                    // TODO mongodb specific fixer-loading problem with binaries
                    'Reading\\BinaryReadMethodsTest::testReadBinaryValue',
                    'Reading\\BinaryReadMethodsTest::testIterateBinaryValue',
                    'Reading\\BinaryReadMethodsTest::testReadBinaryValueAsString',
                    'Reading\\BinaryReadMethodsTest::testReadBinaryValues',
                    'Reading\\BinaryReadMethodsTest::testReadBinaryValuesAsString',
                    'Reading\\PropertyReadMethodsTest::testGetBinary',
                    'Reading\\PropertyReadMethodsTest::testGetBinaryMulti',

                    'Query\QueryManagerTest::testGetQuery',
                    'Query\QueryManagerTest::testGetQueryInvalid',
                    'Query\\NodeViewTest::testSeekable',

                    'Writing\\NamespaceRegistryTest::testRegisterUnregisterNamespace',
                    'Writing\\CopyMethodsTest::testCopyUpdateOnCopy',
        );

    }

    public static function getInstance()
    {
        if (null === self::$instance) {
            self::$instance = new ImplementationLoader();
        }
        return self::$instance;
    }

    public function getRepositoryFactoryParameters()
    {
        global $db; // initialized in bootstrap_mongodb.php
        return array('jackalope.mongodb_database' => $db);
    }

    public function getCredentials() {
        return new \PHPCR\SimpleCredentials($GLOBALS['phpcr.user'], $GLOBALS['phpcr.pass']);
    }

    public function getInvalidCredentials() {
        return new \PHPCR\SimpleCredentials('nonexistinguser', '');
    }

    public function getRestrictedCredentials()
    {
        return new \PHPCR\SimpleCredentials('anonymous', 'abc');
    }

    public function getUserId()
    {
        return $GLOBALS['phpcr.user'];
    }

    public function getTestSupported($chapter, $case, $name)
    {
        // this seems a bug in php with arrayiterator - and jackalope is using
        // arrayiterator for the search result
        // https://github.com/phpcr/phpcr-api-tests/issues/22
        if ('Query\\NodeViewTest::testSeekable' == $name && PHP_MAJOR_VERSION == 5 && PHP_MINOR_VERSION <= 3 && PHP_RELEASE_VERSION <= 3) {
            return false;
        }
        return parent::getTestSupported($chapter, $case, $name);
    }

    function getFixtureLoader()
    {
        global $db; // initialized in bootstrap_mongodb.php
        require_once "MongoDBFixtureLoader.php";
        return new \MongoDBFixtureLoader($db, __DIR__ . "/../fixtures/mongodb/");
    }
}
