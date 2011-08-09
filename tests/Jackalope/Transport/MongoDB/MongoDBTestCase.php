<?php

namespace Jackalope\Transport\MongoDB;

use Jackalope\TestCase;

abstract class MongoDBTestCase extends TestCase
{
    protected $conn;

    public function setUp()
    {
        if (!isset($GLOBALS['phpcr.mongodb.loaded'])) {
            $this->markTestSkipped('phpcr.mongodb.loaded is not set. Skipping MongoDB tests.');
        }
    }
}