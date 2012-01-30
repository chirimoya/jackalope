<?php
/** make sure we get ALL infos from php */
error_reporting(E_ALL | E_STRICT);

// PHPUnit 3.4 compat
if (method_exists('PHPUnit_Util_Filter', 'addDirectoryToFilter')) {
    PHPUnit_Util_Filter::addDirectoryToFilter(__DIR__);
    PHPUnit_Util_Filter::addFileToFilter(__DIR__.'/../src/Jackalope/Transport/curl.php');
}

/**
 * bootstrap file for jackalope doctrine dbal tests
 *
 * If you want to overwrite the defaults, you can to specify the autoloader and doctrine sources
 */
if (isset($GLOBALS['phpcr.doctrine.loader'])) {
    require_once $GLOBALS['phpcr.doctrine.loader'];
    // Make sure we have the necessary config
    $necessaryConfigValues = array('phpcr.doctrine.loader', 'phpcr.doctrine.commondir', 'phpcr.doctrine.dbaldir');
    foreach ($necessaryConfigValues as $val) {
        if (empty($GLOBALS[$val])) {
            die('Please set '.$val.' in your phpunit.xml.' . "\n");
        }
    }
    $loader = new \Doctrine\Common\ClassLoader("Doctrine\Common", $GLOBALS['phpcr.doctrine.commondir']);
    $loader->register();

    $loader = new \Doctrine\Common\ClassLoader("Doctrine\DBAL", $GLOBALS['phpcr.doctrine.dbaldir']);
    $loader->register();
}

/**
 * autoloader: tests rely on an autoloader.
 */
require_once __DIR__ . '/../src/autoload.doctrine_dbal.dist.php';

### Load classes needed for jackalope unit tests ###
require 'Jackalope/TestCase.php';
require 'Jackalope/Transport/Jackrabbit/JackrabbitTestCase.php';
require 'Jackalope/Transport/DoctrineDBAL/DoctrineDBALTestCase.php';

### Load the implementation loader class ###
require 'inc/DoctrineDBALImplementationLoader.php';

/**
 * set up the backend connection
 */
$dbConn = \Doctrine\DBAL\DriverManager::getConnection(array(
    'driver'    => $GLOBALS['phpcr.doctrine.dbal.driver'],
    'host'      => $GLOBALS['phpcr.doctrine.dbal.host'],
    'user'      => $GLOBALS['phpcr.doctrine.dbal.username'],
    'password'  => $GLOBALS['phpcr.doctrine.dbal.password'],
    'dbname'    => $GLOBALS['phpcr.doctrine.dbal.dbname']
));

// TODO: refactor this into the command (a --reset option) and use the command instead
echo "Updating schema...";
$schema = \Jackalope\Transport\DoctrineDBAL\RepositorySchema::create();
foreach ($schema->toDropSql($dbConn->getDatabasePlatform()) as $sql) {
    try {
        $dbConn->exec($sql);
    } catch(PDOException $e) {
    }
}
foreach ($schema->toSql($dbConn->getDatabasePlatform()) as $sql) {
    try {
    $dbConn->exec($sql);
    } catch(PDOException $e) {
        echo $e->getMessage();
    }
}
$GLOBALS['phpcr.doctrine_dbal.loaded'] = true;
echo "done.\n";

/** some constants */

define('SPEC_VERSION_DESC', 'jcr.specification.version');
define('SPEC_NAME_DESC', 'jcr.specification.name');
define('REP_VENDOR_DESC', 'jcr.repository.vendor');
define('REP_VENDOR_URL_DESC', 'jcr.repository.vendor.url');
define('REP_NAME_DESC', 'jcr.repository.name');
define('REP_VERSION_DESC', 'jcr.repository.version');
define('OPTION_TRANSACTIONS_SUPPORTED', 'option.transactions.supported');
define('OPTION_VERSIONING_SUPPORTED', 'option.versioning.supported');
define('OPTION_OBSERVATION_SUPPORTED', 'option.observation.supported');
define('OPTION_LOCKING_SUPPORTED', 'option.locking.supported');
