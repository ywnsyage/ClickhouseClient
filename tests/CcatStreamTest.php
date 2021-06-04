<?php

namespace Ywnsyage\Clickhouse;

use function GuzzleHttp\Psr7\stream_for;
use PHPUnit\Framework\TestCase;
use Ywnsyage\Clickhouse\Support\CcatStream;

/**
 * @covers \Ywnsyage\Clickhouse\Support\CcatStream
 */
class CcatStreamTest extends TestCase
{
    public function testStreamSize()
    {
        $ccatStream = new CcatStream(stream_for('a'), '');

        $this->assertNull($ccatStream->getSize());
    }
}
