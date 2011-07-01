<?php

namespace Jackalope;

abstract class TestCase extends \PHPUnit_Framework_TestCase
{
    protected $config;
    protected $credentials;

    protected $JSON = '{":jcr:primaryType":"Name","jcr:primaryType":"rep:root","jcr:system":{},"tests_level1_access_base":{}}';

    protected function setUp()
    {
        foreach ($GLOBALS as $cfgKey => $value) {
            if ('phpcr.' === substr($cfgKey, 0, 6)) {
                $this->config[substr($cfgKey, 6)] = $value;
            }
        }
        $this->credentials = new \PHPCR\SimpleCredentials($this->config['user'], $this->config['pass']);
    }

    protected function getTransportStub($path)
    {
        $factory = new \Jackalope\Factory;
        $transport = $this->getMock('\Jackalope\Transport\Davex\Client', array('getNode', 'getNodeTypes', 'getNodePathForIdentifier'), array($factory, 'http://example.com'));

        $transport->expects($this->any())
            ->method('getNode')
            ->will($this->returnValue(json_decode($this->JSON)));

        $dom = new \DOMDocument();
        $dom->load(dirname(__FILE__) . '/../fixtures/nodetypes.xml');
        $transport->expects($this->any())
            ->method('getNodeTypes')
            ->will($this->returnValue($dom));

        $transport->expects($this->any())
            ->method('getNodePathForIdentifier')
            ->will($this->returnValue('/jcr:root/uuid/to/path'));

        return $transport;
    }

    protected function getSessionMock()
    {
        $factory = new \Jackalope\Factory;
        return $this->getMock('\Jackalope\Session', array(), array($factory), '', false);
    }

    protected function getNodeTypeManager()
    {
        $factory = new \Jackalope\Factory;
        $dom = new \DOMDocument();
        $dom->load(dirname(__FILE__) . '/../fixtures/nodetypes.xml');
        $converter = new \Jackalope\NodeType\NodeTypeXmlConverter;
        $om = $this->getMock('\Jackalope\ObjectManager', array('getNodeTypes'), array($factory, $this->getTransportStub('/jcr:root'), $this->getSessionMock()));
        $om->expects($this->any())
            ->method('getNodeTypes')
            ->will($this->returnValue($converter->getNodeTypesFromXml($dom)));
        return new \Jackalope\NodeType\NodeTypeManager($factory, $om);
    }
}
