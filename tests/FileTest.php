<?php

namespace Ywnsyage\Clickhouse;

use PHPUnit\Framework\TestCase;
use Ywnsyage\Clickhouse\Common\File;

/**
 * @covers \Ywnsyage\Clickhouse\Common\AbstractFile
 * @covers \Ywnsyage\Clickhouse\Common\File
 */
class FileTest extends TestCase
{
    public function testFile()
    {
        $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $fileContent = [];
        $result = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = $i.PHP_EOL;

            $result[] = $i.PHP_EOL;
        }

        $result = implode('', $result);

        file_put_contents($fileName, implode('', $fileContent));

        $file = new File($fileName);
        $stream = $file->open(false);

        $this->assertEquals($result, $stream->getContents(), 'Correctly reads content from file without encoding');

        unlink($fileName);
    }

    public function testFileWithGzip()
    {
        $fileName = tempnam(sys_get_temp_dir(), 'tbchc_');
        $fileContent = [];
        $result = [];

        for ($i = 0; $i < 100; $i++) {
            $fileContent[] = $i.PHP_EOL;

            $result[] = $i.PHP_EOL;
        }

        $result = implode('', $result);

        file_put_contents($fileName, implode('', $fileContent));

        $file = new File($fileName);
        $stream = $file->open();

        $this->assertEquals($result, gzdecode($stream->getContents()), 'Correctly reads content from file with encoding');

        unlink($fileName);
    }
}
