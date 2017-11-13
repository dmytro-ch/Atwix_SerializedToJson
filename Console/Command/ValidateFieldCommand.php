<?php
/**
 * @author Atwix Team
 * @copyright Copyright (c) 2017 Atwix (https://www.atwix.com/)
 * @package Atwix_SerializedToJson
 */

namespace Atwix\SerializedToJson\Console\Command;

use Exception;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\Db\Select;
use Magento\Framework\Filesystem\Io\File as FileIO;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Magento\Framework\Serialize\Serializer\Json as JsonSerializer;

/**
 * Class ValidateFieldCommand
 */
class ValidateFieldCommand extends Command
{
    /**
     * Argument key for table name
     */
    const ARGUMENT_TABLE = 'table';

    /**
     * Argument key for identifier field name
     */
    const ARGUMENT_ID_FIELD = 'id-field';

    /**
     * Argument key for field name to be processed
     */
    const ARGUMENT_FIELD = 'field';

    /**
     * Page size
     */
    const ROWS_PER_PAGE = 1000;

    /**
     * Output file name
     */
    const OUTPUT_FILE_NAME = 'serialized_to_json_validation.log';

    /**
     * Error message pattern
     */
    const ERROR_MESSAGE_PATTERN = '%s: %s - %s';

    /**
     * Resource Connection
     *
     * @var ResourceConnection
     */
    protected $resource;

    /**
     * Filesystem IO
     *
     * @var FileIO
     */
    protected $ioFile;

    /**
     * Directory list
     *
     * @var DirectoryList
     */
    protected $directoryList;

    /**
     * Json Serializer
     *
     * @var JsonSerializer
     */
    protected $jsonSerializer;

    /**
     * ValidateFieldCommand constructor
     *
     * @param ResourceConnection $resource
     * @param FileIO $ioFile
     * @param DirectoryList $directoryList
     * @param JsonSerializer $jsonSerializer
     * @param null $name
     */
    public function __construct(
        ResourceConnection $resource,
        FileIO $ioFile,
        DirectoryList $directoryList,
        JsonSerializer $jsonSerializer,
        $name = null
    ) {
        parent::__construct($name);
        $this->resource = $resource;
        $this->ioFile = $ioFile;
        $this->directoryList = $directoryList;
        $this->jsonSerializer = $jsonSerializer;
    }

    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $this->setName('atwix:serialized-to-json:validate');
        $this->setDescription('Get list of invalid serialized data');
        $this->addArgument(self::ARGUMENT_TABLE, InputArgument::REQUIRED, 'Table name');
        $this->addArgument(self::ARGUMENT_ID_FIELD, InputArgument::REQUIRED, 'Identifier field name');
        $this->addArgument(self::ARGUMENT_FIELD, InputArgument::REQUIRED, 'Field to be validated');
    }

    /**
     * {@inheritdoc}
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $tableName = $input->getArgument(self::ARGUMENT_TABLE);
        $idFieldName = $input->getArgument(self::ARGUMENT_ID_FIELD);
        $fieldName = $input->getArgument(self::ARGUMENT_FIELD);
        $invalidRecordsCount = 0;
        $resultOutput = [];
        $resultOutput[] = sprintf('Validation result for field "%s" in table "%s":', $fieldName, $tableName);

        $connection = $this->resource->getConnection();

        /** @var Select $countQuery */
        $countQuery = $connection->select();
        $countQuery->from($tableName, ['rows_count' => 'COUNT(*)']);
        $count = (int)$connection->fetchOne($countQuery);
        $pagesCount = ceil($count / self::ROWS_PER_PAGE);

        /** @var Select $select */
        $select = $connection->select();
        $select->from($tableName, [$idFieldName, $fieldName]);

        for ($currPage = 1; $currPage <= $pagesCount; $currPage++) {
            $select->limitPage($currPage, self::ROWS_PER_PAGE);
            $rows = $connection->fetchAll($select);

            foreach ($rows as $row) {
                $rowId = $row[$idFieldName];
                $value = $row[$fieldName];

                if ($value === false || $value === null || $value === '') {
                    // The field should not contain the empty values
                    $row[$fieldName];
                    $invalidRecordsCount++;
                    $resultOutput[] = sprintf(self::ERROR_MESSAGE_PATTERN, $idFieldName, $rowId, 'contains empty value');

                    continue;
                }

                if ($this->isValidJson($value)) {
                    // If the field contains valid JSON, it should not be considered as an invalid record
                    continue;
                }

                try {
                    // Try to unserialize field value
                    unserialize($value);
                } catch (Exception $e) {
                    $invalidRecordsCount++;
                    $resultOutput[] = sprintf(self::ERROR_MESSAGE_PATTERN, $idFieldName, $rowId, $e->getMessage());
                }
            }
        }

        $summaryMessage = sprintf('Invalid records count: %s', $invalidRecordsCount);
        $resultOutput[] = $summaryMessage;

        $filePath = $this->directoryList->getPath(DirectoryList::LOG) . DIRECTORY_SEPARATOR . self::OUTPUT_FILE_NAME;
        $this->ioFile->checkAndCreateFolder(dirname($filePath));
        $this->ioFile->open();
        $this->ioFile->write($filePath, implode($resultOutput, PHP_EOL));
        $this->ioFile->close();

        $output->writeln($summaryMessage);
        $output->writeln(sprintf('Result output: %s', $filePath));

        return 0;
    }

    /**
     * Check whether the value has been already converted to json
     *
     * @param string $value
     *
     * @return bool
     */
    protected function isValidJson($value)
    {
        try {
            $this->jsonSerializer->unserialize($value);
        } catch (Exception $e) {
            return false;
        }

        return true;
    }
}