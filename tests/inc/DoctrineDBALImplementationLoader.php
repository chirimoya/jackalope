<?php

require_once __DIR__.'/../phpcr-api/inc/AbstractLoader.php';

/**
 * Implementation loader for jackalope-doctrine-dbal
 */
class ImplementationLoader extends \PHPCR\Test\AbstractLoader
{
    private static $instance = null;

    protected function __construct()
    {
        parent::__construct('Jackalope\RepositoryFactoryDoctrineDBAL', $GLOBALS['phpcr.workspace']);

        $this->unsupportedChapters = array(
                    'PermissionsAndCapabilities',
                    'Import',
                    'Observation',
                    'WorkspaceManagement',
                    'ShareableNodes',
                    'Versioning',
                    'AccessControlManagement',
                    'Locking',
                    'LifecycleManagement',
                    'RetentionAndHold',
                    'Transactions',
                    'SameNameSiblings',
                    'OrderableChildNodes',
        );

        $this->unsupportedCases = array(
                    'Writing\\MoveMethodsTest',
                    'Writing\\NodeTypePreemptiveValidationTest', // TODO: some of this could work, test it and make all work
        );

        $this->unsupportedTests = array(
                    'Connecting\\RepositoryTest::testLoginException', //TODO: figure out what would be invalid credentials
                    'Connecting\\RepositoryTest::testNoLogin',
                    'Connecting\\RepositoryTest::testNoLoginAndWorkspace',

                    'Reading\\SessionReadMethodsTest::testImpersonate', //TODO: Check if that's implemented in newer jackrabbit versions.
                    'Reading\\SessionNamespaceRemappingTest::testSetNamespacePrefix',
                    'Reading\\NodeReadMethodsTest::testGetSharedSetUnreferenced', // TODO: should this be moved to 14_ShareableNodes
                    'Reading\\PropertyReadMethodsTest::testJcrCreated', // fails because NodeTypeDefinitions do not work inside DoctrineDBAL transport yet.

                    'Query\\QueryManagerTest::testGetQuery',
                    'Query\\QueryManagerTest::testGetQueryInvalid',
                    'Query\\QueryObjectSql2Test::testGetStoredQueryPath',
                    'Query\\QuerySql2OperationsTest::testQueryJoin',
                    'Query\\QuerySql2OperationsTest::testQueryJoinReference',

                    'Writing\\NamespaceRegistryTest::testRegisterUnregisterNamespace',
                    'Writing\\CopyMethodsTest::testCopyUpdateOnCopy',
                    'Writing\\MoveMethodsTest::testSessionDeleteMoved', // TODO: enable and look at the exception you get as starting point
                    'Writing\\MoveMethodsTest::testNodeOrderBeforeEnd',
                    'Writing\\MoveMethodsTest::testNodeOrderBeforeDown',
                    'Writing\\MoveMethodsTest::testNodeOrderBeforeUp',
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
        global $dbConn; // initialized in bootstrap_doctrine_dbal.php
        return array('jackalope.doctrine_dbal_connection' => $dbConn);
    }

    public function getCredentials()
    {
        return new \PHPCR\SimpleCredentials($GLOBALS['phpcr.user'], $GLOBALS['phpcr.pass']);
    }

    public function getInvalidCredentials()
    {
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

    function getRepository()
    {
        global $dbConn;

        $dbConn->insert('phpcr_workspaces', array('name' => $GLOBALS['phpcr.workspace']));
        $transport = new \Jackalope\Transport\DoctrineDBAL\Client(new \Jackalope\Factory, $dbConn);
        $GLOBALS['pdo'] = $dbConn->getWrappedConnection();
        return new \Jackalope\Repository(null, $transport);
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
        require_once "DoctrineDBALFixtureLoader.php";
        return new DoctrineDBALFixtureLoader($GLOBALS['pdo'], __DIR__ . "/../fixtures/doctrine/");
    }
}
